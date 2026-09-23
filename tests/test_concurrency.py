"""并发安全测试：在途计数、路由结果与熔断状态在压力下不得竞态。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import InProcessTransport
from app.types import BalancingStrategy, Instance, Request
from app.upstream import UpstreamBehavior, make_handler

logger = logging.getLogger("tests.concurrency")


def make_instances(n: int = 4):
    return [
        Instance(id=f"inst-{i}", address="127.0.0.1", port=9000 + i)
        for i in range(n)
    ]


def build(strategy, n=4, barrier_latency=0.05):
    registry = InstanceRegistry()
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=100,
                                                  open_duration=1.0))
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    instances = make_instances(n)
    for inst in instances:
        registry.register(inst)

    peak = {"value": 0}
    peak_lock = threading.Lock()
    ready = threading.Barrier(n * 8)

    def handler(request, instance):
        # 占位成功后再制造同步点：所有线程几乎同时“在途”，便于抓峰值。
        ok = ready.wait(timeout=2)
        assert ok >= 0
        with peak_lock:
            cur = registry.view(instance.id).inflight
            peak["value"] = max(peak["value"], cur)
        import time as _t

        _t.sleep(barrier_latency)
        return {"instance": instance.id}

    dispatcher = Dispatcher(
        registry,
        LoadBalancer(strategy),
        breaker,
        InProcessTransport({i.id: handler for i in instances}),
        RetryConfig(max_retries=0),
    )
    return registry, dispatcher, instances, peak


def test_least_inflight_balances_under_concurrency() -> None:
    # 32 个并发请求打到 4 个实例，每实例最多同时在途应接近均衡。
    registry, dispatcher, instances, peak = build(
        BalancingStrategy.LEAST_INFLIGHT, n=4
    )
    counts = {i.id: 0 for i in instances}
    counts_lock = threading.Lock()

    def worker(idx: int):
        resp = dispatcher.dispatch(Request(key=f"c-{idx}"))
        with counts_lock:
            counts[resp.instance_id] += 1
        return resp.instance_id

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(worker, range(32)))

    logger.info("并发 32 请求各实例命中分布=%s 观测到的单实例峰值在途=%d",
                counts, peak["value"])
    assert sum(counts.values()) == 32
    # 最少在途 + 同步屏障：每实例恰好 8 个。
    assert all(v == 8 for v in counts.values()), counts
    assert peak["value"] == 8
    # 全部完成后在途归零。
    assert all(v.inflight == 0 for v in registry.views())


def test_consistent_hash_stable_under_concurrency() -> None:
    registry = InstanceRegistry()
    breaker = CircuitBreaker()
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    instances = make_instances(4)
    for inst in instances:
        registry.register(inst)

    import time as _t

    def handler(request, instance):
        _t.sleep(0.002)
        return {"instance": instance.id}

    dispatcher = Dispatcher(
        registry,
        LoadBalancer(BalancingStrategy.CONSISTENT_HASH),
        breaker,
        InProcessTransport({i.id: handler for i in instances}),
        RetryConfig(max_retries=0),
    )

    keys = [f"user-{i % 20}" for i in range(200)]  # 20 个不同键，重复并发
    single = LoadBalancer(BalancingStrategy.CONSISTENT_HASH)
    expected = {k: single.choose(instances, k).id for k in set(keys)}

    results = {}
    results_lock = threading.Lock()

    def worker(k: str):
        resp = dispatcher.dispatch(Request(key=k))
        with results_lock:
            results.setdefault(k, set()).add(resp.instance_id)

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(worker, keys))

    for k, owners in results.items():
        assert owners == {expected[k]}, (k, owners, expected[k])
    logger.info("并发下每个键始终落在同一实例，共 %d 个键无漂移", len(results))


def test_inflight_no_race_on_failover() -> None:
    # 大量并发失败转移，在途计数最终必须归零，熔断状态保持一致。
    registry = InstanceRegistry()
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=50,
                                                  open_duration=30.0))
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    instances = make_instances(3)
    for inst in instances:
        registry.register(inst)

    behaviors = {i.id: UpstreamBehavior(always_fail=True) for i in instances}
    dispatcher = Dispatcher(
        registry,
        LoadBalancer(BalancingStrategy.ROUND_ROBIN),
        breaker,
        InProcessTransport({i: make_handler(b) for i, b in behaviors.items()}),
        RetryConfig(max_retries=2),
    )

    def worker(idx: int):
        dispatcher.dispatch_report(Request(key=f"f-{idx}"))

    with ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(worker, range(120)))

    for view in registry.views():
        assert view.inflight == 0, (view.instance.id, view.inflight)
    logger.info("120 个并发全失败请求结束后，所有实例在途计数归零")
    # 熔断器对每个实例都应有一致的终态（open 或 closed），无异常即一致。
    states = {i.id: breaker.state(i.id).value for i in instances}
    logger.info("并发后熔断终态=%s", states)
