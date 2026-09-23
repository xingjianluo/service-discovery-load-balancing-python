"""实例注册、摘除、健康状态收敛与恢复测试。"""
from __future__ import annotations

import logging

import pytest

from app.health import HealthChecker, HealthCheckConfig
from app.registry import InstanceRegistry
from app.types import Instance, InstanceState, UnavailableReason

logger = logging.getLogger("tests.registry")


def make_instances():
    return [
        Instance(id="a", address="127.0.0.1", port=9001),
        Instance(id="b", address="127.0.0.1", port=9002),
        Instance(id="c", address="127.0.0.1", port=9003),
    ]


def test_register_and_candidates() -> None:
    registry = InstanceRegistry()
    for inst in make_instances():
        registry.register(inst)
    assert [i.id for i in registry.candidates()] == ["a", "b", "c"]


def test_deregister_excludes_instance() -> None:
    registry = InstanceRegistry()
    for inst in make_instances():
        registry.register(inst)

    registry.deregister("b")
    logger.info("人工摘除实例 b，候选=%s", [i.id for i in registry.candidates()])

    assert [i.id for i in registry.candidates()] == ["a", "c"]
    view = registry.view("b")
    assert view.state is InstanceState.DEREGISTERED
    assert view.state_reason == UnavailableReason.DEREGISTERED.value
    # 摘除后不能再占在途名额。
    assert registry.increment_inflight("b") is False
    assert view.inflight == 0


def test_reregister_recovers_from_deregistered() -> None:
    registry = InstanceRegistry()
    for inst in make_instances():
        registry.register(inst)
    registry.deregister("b")
    # 健康探测不应自动恢复被人工摘除的实例。
    registry.mark_healthy("b")
    assert registry.view("b").state is InstanceState.DEREGISTERED
    # 重新注册后恢复。
    registry.register(Instance(id="b", address="127.0.0.1", port=9002))
    assert registry.view("b").state is InstanceState.HEALTHY
    logger.info("实例 b 重新注册后状态 -> %s", registry.view("b").state.value)


def test_health_consecutive_failures_then_recovery() -> None:
    registry = InstanceRegistry()
    for inst in make_instances():
        registry.register(inst)

    alive = {"a": True, "b": True, "c": True}
    checker = HealthChecker(
        registry,
        probe=lambda inst: alive[inst.id],
        config=HealthCheckConfig(
            interval=0.01, failure_threshold=3, success_threshold=2
        ),
    )

    alive["b"] = False
    for n in range(3):
        checker.check_once()
    view = registry.view("b")
    assert view.state is InstanceState.UNHEALTHY
    assert view.state_reason == UnavailableReason.PROBE_FAILED.value
    assert view.consecutive_failures == 3
    logger.info(
        "b 连续探测失败 state=%s reason=%s failures=%d",
        view.state.value,
        view.state_reason,
        view.consecutive_failures,
    )

    # 未达 success_threshold（2 次）前保持不健康。
    alive["b"] = True
    checker.check_once()
    assert registry.view("b").state is InstanceState.UNHEALTHY
    checker.check_once()
    assert registry.view("b").state is InstanceState.HEALTHY
    logger.info("b 连续探测成功 2 次后 state=healthy，重新参与调度")


def test_deregistered_not_probed() -> None:
    registry = InstanceRegistry()
    registry.register(Instance(id="a", address="127.0.0.1", port=9001))
    registry.deregister("a")

    def boom(_inst: Instance) -> bool:
        raise AssertionError("不应探测已摘除实例")

    checker = HealthChecker(registry, probe=boom)
    checker.check_once()  # 不抛异常即通过
    assert registry.view("a").state is InstanceState.DEREGISTERED
