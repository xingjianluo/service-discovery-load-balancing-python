"""端到端：真实 HTTP 上游（本地线程进程模拟）+ 健康探测 + 故障转移。"""
from __future__ import annotations

import logging
import time

import pytest

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.health import HealthChecker, HealthCheckConfig, http_probe
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import HttpTransport
from app.types import BalancingStrategy, Instance, Request
from app.upstream import LocalUpstreamServer, UpstreamBehavior

logger = logging.getLogger("tests.e2e")


class _Cluster:
    def __init__(self):
        self.registry = InstanceRegistry()
        self.breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=5, open_duration=0.5)
        )
        self.registry.set_circuit_provider(
            lambda i: (self.breaker.state(i), self.breaker.open_until(i))
        )
        self.servers: dict[str, LocalUpstreamServer] = {}
        self.behaviors: dict[str, UpstreamBehavior] = {}

    def add(self, instance_id: str, port: int, behavior: UpstreamBehavior):
        server = LocalUpstreamServer(instance_id, port, behavior)
        server.start()
        self.servers[instance_id] = server
        self.behaviors[instance_id] = behavior
        self.registry.register(
            Instance(id=instance_id, address="127.0.0.1", port=port)
        )

    def stop(self):
        for server in self.servers.values():
            server.stop()


@pytest.fixture()
def cluster():
    c = _Cluster()
    yield c
    c.stop()


def test_http_round_robin_and_deregister(cluster) -> None:
    cluster.add("a", 19101, UpstreamBehavior())
    cluster.add("b", 19102, UpstreamBehavior())
    cluster.add("c", 19103, UpstreamBehavior())
    dispatcher = Dispatcher(
        cluster.registry,
        LoadBalancer(BalancingStrategy.ROUND_ROBIN),
        cluster.breaker,
        HttpTransport(),
        RetryConfig(max_retries=1),
    )

    picked = [
        dispatcher.dispatch(Request(key=f"r-{i}", timeout=2.0)).instance_id
        for i in range(6)
    ]
    logger.info("真实 HTTP 轮询结果=%s", picked)
    assert picked == ["a", "b", "c", "a", "b", "c"]

    cluster.registry.deregister("b")
    for i in range(4):
        resp = dispatcher.dispatch(Request(key=f"d-{i}", timeout=2.0))
        assert resp.instance_id in {"a", "c"}
    logger.info("摘除 b 后真实 HTTP 请求仅落在 a/c")


def test_http_health_remove_and_recover(cluster) -> None:
    cluster.add("a", 19201, UpstreamBehavior())
    cluster.add("b", 19202, UpstreamBehavior())
    cluster.add("c", 19203, UpstreamBehavior())
    dispatcher = Dispatcher(
        cluster.registry,
        LoadBalancer(BalancingStrategy.ROUND_ROBIN),
        cluster.breaker,
        HttpTransport(),
        RetryConfig(max_retries=1),
    )
    checker = HealthChecker(
        cluster.registry,
        http_probe("/health", timeout=0.5),
        HealthCheckConfig(interval=0.05, failure_threshold=2,
                          success_threshold=2),
    )

    # b 下线，健康探测应收敛为 unhealthy 并停止接收流量。
    cluster.behaviors["b"].healthy = False
    for _ in range(6):
        checker.check_once()
        time.sleep(0.03)
    assert cluster.registry.view("b").state.value == "unhealthy"
    logger.info("b /health 持续 503 -> state=unhealthy reason=probe_failed")

    for i in range(6):
        resp = dispatcher.dispatch(Request(key=f"h-{i}", timeout=2.0))
        assert resp.instance_id != "b"

    # 恢复后重新参与调度。
    cluster.behaviors["b"].healthy = True
    for _ in range(6):
        checker.check_once()
        time.sleep(0.03)
    assert cluster.registry.view("b").state.value == "healthy"
    logger.info("b /health 恢复 200 -> state=healthy，重新参与调度")


def test_http_failover_on_500(cluster) -> None:
    cluster.add("a", 19301, UpstreamBehavior(fail_times=1))
    cluster.add("b", 19302, UpstreamBehavior())
    cluster.add("c", 19303, UpstreamBehavior())
    dispatcher = Dispatcher(
        cluster.registry,
        LoadBalancer(BalancingStrategy.ROUND_ROBIN),
        cluster.breaker,
        HttpTransport(),
        RetryConfig(max_retries=2),
    )
    resp = dispatcher.dispatch(Request(key="e2e-fail", timeout=2.0))
    assert resp.instance_id != "a"
    assert resp.attempts == 2
    assert resp.retries == 1
    logger.info(
        "真实 HTTP：a 返回 500 后转移到 %s（attempts=%d retries=%d）",
        resp.instance_id, resp.attempts, resp.retries,
    )


def test_http_connection_refused_is_retryable(cluster) -> None:
    # 注册一个没有任何进程监听的实例（端口 1）+ 一个健康实例。
    # 用一致性哈希找到归属 ghost 的键，验证连接拒绝会被当成可重试失败并转移。
    cluster.registry.register(
        Instance(id="ghost", address="127.0.0.1", port=1)
    )
    cluster.add("a", 19401, UpstreamBehavior())
    from app.load_balancer import _ConsistentHashRing

    ring = _ConsistentHashRing()
    candidates = cluster.registry.candidates()
    ghost_key = next(
        f"k{i}"
        for i in range(10000)
        if ring.get_owner(candidates, f"k{i}") == "ghost"
    )
    dispatcher = Dispatcher(
        cluster.registry,
        LoadBalancer(BalancingStrategy.CONSISTENT_HASH),
        cluster.breaker,
        HttpTransport(),
        RetryConfig(max_retries=2),
    )
    report = dispatcher.dispatch_report(Request(key=ghost_key, timeout=1.0))
    assert report.success
    assert report.selected[0] == "ghost"
    assert report.selected[-1] == "a"
    logger.info(
        "连接拒绝实例 ghost 被跳过，key=%s 尝试序列=%s，最终=%s",
        ghost_key, report.selected, report.selected[-1],
    )
