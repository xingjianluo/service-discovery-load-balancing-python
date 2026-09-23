"""故障转移、重试约束与熔断测试：最终结果与原因归类必须正确。"""
from __future__ import annotations

import logging
import time

import pytest

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import InProcessTransport
from app.types import (
    AllInstancesUnavailable,
    BalancingStrategy,
    Instance,
    Request,
    UnavailableReason,
)
from app.upstream import UpstreamBehavior, make_handler

logger = logging.getLogger("tests.failover")


def make_instances():
    return [
        Instance(id="a", address="127.0.0.1", port=1),
        Instance(id="b", address="127.0.0.1", port=2),
        Instance(id="c", address="127.0.0.1", port=3),
    ]


def build_dispatcher(strategy=BalancingStrategy.ROUND_ROBIN, behaviors=None,
                     max_retries=2, cb_config=None):
    behaviors = behaviors or {i: UpstreamBehavior() for i in ("a", "b", "c")}
    registry = InstanceRegistry()
    breaker = CircuitBreaker(cb_config or CircuitBreakerConfig())
    registry.set_circuit_provider(
        lambda i: (breaker.state(i), breaker.open_until(i))
    )
    for inst in make_instances():
        registry.register(inst)
    dispatcher = Dispatcher(
        registry,
        LoadBalancer(strategy),
        breaker,
        InProcessTransport({i: make_handler(b) for i, b in behaviors.items()}),
        RetryConfig(max_retries=max_retries),
    )
    return registry, breaker, dispatcher


def test_retryable_failure_fails_over() -> None:
    # a 第一次调用失败，轮询首位选中 a，随后故障转移。
    behaviors = {
        "a": UpstreamBehavior(fail_times=1),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    registry, breaker, dispatcher = build_dispatcher(behaviors=behaviors)
    resp = dispatcher.dispatch(Request(key="order-1001"))
    logger.info(
        "key=order-1001 最终实例=%s 尝试=%d 重试=%d",
        resp.instance_id, resp.attempts, resp.retries,
    )
    assert resp.instance_id != "a"
    assert resp.attempts == 2
    assert resp.retries == 1


def test_timeout_is_retryable() -> None:
    behaviors = {
        "a": UpstreamBehavior(timeout_times=1),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    _, _, dispatcher = build_dispatcher(behaviors=behaviors)
    report = dispatcher.dispatch_report(Request(key="timeout-key"))
    assert report.success
    assert report.selected[0] == "a"
    assert report.selected[1] != "a"
    assert report.reason is None
    logger.info("key=timeout-key 超时后转移到 %s", report.selected[1])


def test_retry_budget_is_bounded_and_no_repeat_instance() -> None:
    behaviors = {i: UpstreamBehavior(always_fail=True) for i in ("a", "b", "c")}
    _, _, dispatcher = build_dispatcher(behaviors=behaviors, max_retries=2)
    report = dispatcher.dispatch_report(Request(key="boom"))
    assert not report.success
    assert report.attempts == 3            # 1 + 2 次重试
    assert report.retries == 2
    assert len(set(report.selected)) == 3  # 不重复打到同一实例
    assert report.reason is UnavailableReason.RETRIES_EXHAUSTED
    logger.info(
        "key=boom 全部失败 尝试序列=%s 最终原因=%s",
        report.selected, report.reason.value,
    )
    with pytest.raises(AllInstancesUnavailable):
        dispatcher.dispatch(Request(key="boom2"))


def test_fatal_error_not_retried() -> None:
    behaviors = {
        "a": UpstreamBehavior(fatal_times=1),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    _, _, dispatcher = build_dispatcher(behaviors=behaviors)
    report = dispatcher.dispatch_report(Request(key="bad-request"))
    assert not report.success
    assert report.attempts == 1
    assert report.retries == 0
    logger.info("key=bad-request 不可重试失败，不转移，原因=%s", report.reason)


def test_circuit_opens_and_instance_skipped() -> None:
    # 熔断器阈值设小：实例连续失败后 OPEN，后续请求直接跳过它。
    behaviors = {
        "a": UpstreamBehavior(always_fail=True),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    registry, breaker, dispatcher = build_dispatcher(
        behaviors=behaviors,
        cb_config=CircuitBreakerConfig(
            failure_threshold=3, open_duration=10.0, half_open_max_calls=1
        ),
    )
    # 打 3 次（每次都会把 3 个实例试一遍），a 累计失败达到阈值。
    for n in range(3):
        dispatcher.dispatch_report(Request(key=f"warm-{n}"))
    assert breaker.state("a").value == "open"
    logger.info("a 熔断打开 until=%s", breaker.open_until("a"))

    # 摘除 b/c 之外的对照：现在只看 a 是否被跳过——
    # 构造一个仅 a 健康但熔断打开的局面，应无候选。
    registry.deregister("b")
    registry.deregister("c")
    report = dispatcher.dispatch_report(Request(key="only-a"))
    assert not report.success
    assert report.selected == []
    assert report.reason is UnavailableReason.NO_CANDIDATE
    logger.info("仅剩熔断实例 a 时原因归类=%s", report.reason.value)


def test_circuit_half_open_recovers() -> None:
    behaviors = {
        "a": UpstreamBehavior(always_fail=True),
        "b": UpstreamBehavior(),
        "c": UpstreamBehavior(),
    }
    registry, breaker, dispatcher = build_dispatcher(
        behaviors=behaviors,
        cb_config=CircuitBreakerConfig(
            failure_threshold=3, open_duration=0.05, half_open_max_calls=1
        ),
    )
    for n in range(3):
        dispatcher.dispatch_report(Request(key=f"warm-{n}"))
    assert breaker.state("a").value == "open"
    time.sleep(0.08)
    # 熔断进入半开：a 已恢复正常行为，探测成功后回到 closed。
    behaviors["a"].always_fail = False
    registry.deregister("b")
    registry.deregister("c")
    resp = dispatcher.dispatch(Request(key="after-recovery"))
    assert resp.instance_id == "a"
    assert breaker.state("a").value == "closed"
    logger.info("a 半开探测成功 -> closed，key=after-recovery 落在 a")
