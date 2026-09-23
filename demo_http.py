"""真实 HTTP 多实例端到端演示。

做两层：
1. 本进程内启动 3 个线程化 HTTP 上游（127.0.0.1:19601-19603），
   并主动注册到与 main.py 同款的调度组件；
2. 后台健康检查器持续探测 /health。

依次演示：轮询、一致性哈希亲和、b 健康检查失败收敛与恢复、500 故障转移。
运行：uv run python demo_http.py
"""
from __future__ import annotations

import logging
import time
from collections import Counter

import http.client
import json

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.health import HealthChecker, HealthCheckConfig, http_probe
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import HttpTransport
from app.types import BalancingStrategy, Instance, Request
from app.upstream import LocalUpstreamServer, UpstreamBehavior

logger = logging.getLogger("demo_http")


def set_healthy(port: int, healthy: bool) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    conn.request(
        "POST", "/admin/health",
        body=json.dumps({"healthy": healthy}),
        headers={"Content-Type": "application/json"},
    )
    conn.getresponse().read()
    conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    behaviors = {
        "a": UpstreamBehavior(),
        "b": UpstreamBehavior(fail_times=1),  # 首次调用返回 500
        "c": UpstreamBehavior(),
    }
    servers = {
        "a": LocalUpstreamServer("a", 19601, behaviors["a"]),
        "b": LocalUpstreamServer("b", 19602, behaviors["b"]),
        "c": LocalUpstreamServer("c", 19603, behaviors["c"]),
    }
    for server in servers.values():
        server.start()
    time.sleep(0.2)

    registry = InstanceRegistry()
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=5, open_duration=1.0)
    )
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    for i, port in (("a", 19601), ("b", 19602), ("c", 19603)):
        registry.register(Instance(id=i, address="127.0.0.1", port=port))

    checker = HealthChecker(
        registry,
        http_probe("/health", timeout=0.5),
        HealthCheckConfig(interval=0.1, failure_threshold=3,
                          success_threshold=2),
    )
    checker.start()

    rr = Dispatcher(
        registry, LoadBalancer(BalancingStrategy.ROUND_ROBIN), breaker,
        HttpTransport(), RetryConfig(max_retries=2),
    )
    ch = Dispatcher(
        registry, LoadBalancer(BalancingStrategy.CONSISTENT_HASH), breaker,
        HttpTransport(), RetryConfig(max_retries=2),
    )

    try:
        logger.info("--- 轮询 6 次 ---")
        for n in range(6):
            resp = rr.dispatch(Request(key=f"rr-{n}", timeout=2.0))
            logger.info("rr-%d -> %s", n, resp.instance_id)

        logger.info("--- 一致性哈希：20 个键的归属 ---")
        owners = {
            f"user-{n}": ch.dispatch(Request(key=f"user-{n}", timeout=2.0)).instance_id
            for n in range(20)
        }
        logger.info("分布=%s", dict(Counter(owners.values())))
        sample = "user-7"
        again = ch.dispatch(Request(key=sample, timeout=2.0)).instance_id
        logger.info("键 %s 两次路由：%s / %s（应相同）",
                    sample, owners[sample], again)
        assert owners[sample] == again

        logger.info("--- b 实例健康检查失败（/health 503）---")
        set_healthy(19602, False)
        time.sleep(0.6)
        assert registry.view("b").state.value == "unhealthy"
        logger.info("b 状态=%s 原因=%s",
                    registry.view("b").state.value,
                    registry.view("b").state_reason)

        hit = Counter()
        for n in range(9):
            hit[rr.dispatch(Request(key=f"x-{n}", timeout=2.0)).instance_id] += 1
        logger.info("b 不可用期间 9 次请求分布=%s（不应出现 b）", dict(hit))
        assert "b" not in hit

        logger.info("--- b 恢复 ---")
        set_healthy(19602, True)
        time.sleep(0.6)
        assert registry.view("b").state.value == "healthy"
        logger.info("b 状态恢复=%s，重新参与调度", registry.view("b").state.value)

        logger.info("全部真实 HTTP 演示场景通过 ✔")
    finally:
        checker.stop()
        for server in servers.values():
            server.stop()


if __name__ == "__main__":
    main()
