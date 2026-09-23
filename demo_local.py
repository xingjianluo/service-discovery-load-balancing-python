"""本地可验证演示：用本地进程/处理器模拟多个上游实例。

运行：uv run python demo_local.py
依次演示：注册发现、摘除、健康收敛、三种择路策略、故障转移与恢复。
"""
from __future__ import annotations

import logging
import time

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.health import HealthChecker, HealthCheckConfig
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import InProcessTransport
from app.types import (
    BalancingStrategy,
    Instance,
    Request,
    UnavailableReason,
)
from app.upstream import UpstreamBehavior, make_handler

logger = logging.getLogger("demo")


def build(strategy: BalancingStrategy, behaviors: dict[str, UpstreamBehavior]):
    registry = InstanceRegistry()
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=3, open_duration=1.0)
    )
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    lb = LoadBalancer(strategy)
    transport = InProcessTransport(
        {i: make_handler(b) for i, b in behaviors.items()}
    )
    dispatcher = Dispatcher(
        registry,
        lb,
        breaker,
        transport,
        RetryConfig(max_retries=2),
    )
    return registry, breaker, dispatcher


def instances():
    return [
        Instance(id="a", address="127.0.0.1", port=9001),
        Instance(id="b", address="127.0.0.1", port=9002),
        Instance(id="c", address="127.0.0.1", port=9003),
    ]


def show_views(registry: InstanceRegistry) -> None:
    for v in registry.views():
        logger.info(
            "  view id=%s state=%s reason=%s failures=%d inflight=%d circuit=%s",
            v.instance.id,
            v.state.value,
            v.state_reason,
            v.consecutive_failures,
            v.inflight,
            v.circuit.value,
        )


def demo_round_robin_and_deregister() -> None:
    logger.info("=== 场景1：轮询 + 人工摘除 b ===")
    behaviors = {i: UpstreamBehavior() for i in ("a", "b", "c")}
    registry, _, dispatcher = build(BalancingStrategy.ROUND_ROBIN, behaviors)
    for inst in instances():
        registry.register(inst)

    for n in range(6):
        resp = dispatcher.dispatch(Request(key=f"req-{n}"))
        logger.info("req-%d -> %s", n, resp.instance_id)

    registry.deregister("b")
    logger.info("已摘除 b，后续请求只应落在 a/c：")
    got = set()
    for n in range(6):
        resp = dispatcher.dispatch(Request(key=f"req-after-{n}"))
        got.add(resp.instance_id)
        assert resp.instance_id != "b", "摘除实例仍被调度！"
    assert got == {"a", "c"}
    show_views(registry)


def demo_consistent_hash() -> None:
    logger.info("=== 场景2：按键一致性哈希亲和性 ===")
    behaviors = {i: UpstreamBehavior() for i in ("a", "b", "c")}
    registry, _, dispatcher = build(BalancingStrategy.CONSISTENT_HASH, behaviors)
    for inst in instances():
        registry.register(inst)

    keys = [f"user-{n}" for n in range(20)]
    first = {k: dispatcher.dispatch(Request(key=k)).instance_id for k in keys}
    again = {k: dispatcher.dispatch(Request(key=k)).instance_id for k in keys}
    assert first == again, "同键在实例集合不变时发生漂移！"
    for k in keys:
        logger.info("key=%s -> %s", k, first[k])

    # 摘除一个实例后，统计受影响键的比例（应远少于全量迁移）。
    victim = first[keys[0]]
    registry.deregister(victim)
    moved = sum(
        1 for k in keys if dispatcher.dispatch(Request(key=k)).instance_id != first[k]
    )
    logger.info("摘除 %s 后受影响键=%d/%d", victim, moved, len(keys))
    assert moved < len(keys)


def demo_failover() -> None:
    logger.info("=== 场景3：可重试失败触发故障转移 ===")
    behaviors = {
        "a": UpstreamBehavior(fail_times=1),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    registry, _, dispatcher = build(BalancingStrategy.ROUND_ROBIN, behaviors)
    for inst in instances():
        registry.register(inst)

    # 直接打到 a（轮询首位），a 首次失败，转移到下一个。
    resp = dispatcher.dispatch(Request(key="k1"))
    logger.info(
        "最终实例=%s attempts=%d retries=%d",
        resp.instance_id,
        resp.attempts,
        resp.retries,
    )
    assert resp.instance_id != "a"
    assert resp.retries == 1

    logger.info("=== 场景4：全部失败 -> 重试耗尽，原因可解释 ===")
    behaviors2 = {i: UpstreamBehavior(always_fail=True) for i in ("a", "b", "c")}
    registry2, _, dispatcher2 = build(BalancingStrategy.ROUND_ROBIN, behaviors2)
    for inst in instances():
        registry2.register(inst)
    report = dispatcher2.dispatch_report(Request(key="k2"))
    logger.info(
        "success=%s attempts=%d retries=%d reason=%s tried=%s",
        report.success,
        report.attempts,
        report.retries,
        report.reason.value if report.reason else None,
        report.selected,
    )
    assert not report.success
    assert report.reason == UnavailableReason.RETRIES_EXHAUSTED
    assert report.attempts == 3  # 首次 + 2 次重试，且不重复同一实例


def demo_health_convergence() -> None:
    logger.info("=== 场景5：健康探测失败收敛与自动恢复（进程内模拟探测） ===")
    behaviors = {i: UpstreamBehavior() for i in ("a", "b", "c")}
    registry, _, dispatcher = build(BalancingStrategy.ROUND_ROBIN, behaviors)
    for inst in instances():
        registry.register(inst)

    alive = {"a": True, "b": True, "c": True}
    checker = HealthChecker(
        registry,
        probe=lambda inst: alive.get(inst.id, False),
        config=HealthCheckConfig(),
    )

    alive["b"] = False
    for _ in range(5):
        checker.check_once()
    assert not registry.is_healthy("b")
    view = registry.view("b")
    assert view.state_reason == UnavailableReason.PROBE_FAILED.value
    logger.info("b 连续探测失败后状态=%s 原因=%s", view.state.value, view.state_reason)

    for n in range(6):
        resp = dispatcher.dispatch(Request(key=f"h-{n}"))
        assert resp.instance_id != "b"

    alive["b"] = True
    for _ in range(3):
        checker.check_once()
    assert registry.is_healthy("b"), "恢复后应重新参与调度"
    logger.info("b 恢复后状态=%s", registry.view("b").state.value)
    show_views(registry)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    demo_round_robin_and_deregister()
    demo_consistent_hash()
    demo_failover()
    demo_health_convergence()
    logger.info("全部本地演示场景通过 ✔")


if __name__ == "__main__":
    main()
