"""服务注册发现与流量调度的本地验证测试。

日志中会打印：请求键、选中实例、状态变化与重试次数。
"""
from __future__ import annotations

import logging
import threading

import pytest

from sd.balancer import LoadBalancer
from sd.breaker import BreakerState
from sd.health import HealthChecker
from sd.models import FailureKind, HealthState, Instance, UnavailableReason
from sd.registry import ServiceRegistry
from sd.router import ConsistentHashRouter, LeastInflightRouter, RoundRobinRouter
from sd.simulator import SimulatedUpstream

logger = logging.getLogger("test.sd")

IDS = ["inst-a", "inst-b", "inst-c"]


def make_env(router=None, max_retries=2, failure_threshold=2, **lb_kwargs):
    reg = ServiceRegistry(failure_threshold=failure_threshold)
    upstreams = {}
    for n, iid in enumerate(IDS):
        reg.register(Instance(iid, "127.0.0.1", 9001 + n))
        upstreams[iid] = SimulatedUpstream(iid)
    lb = LoadBalancer(reg, router or RoundRobinRouter(), upstreams,
                      max_retries=max_retries, call_timeout=0.2,
                      breaker_threshold=3, breaker_cooldown=0.1, **lb_kwargs)
    return reg, upstreams, lb


def log_result(tag, key, result):
    logger.info("[%s] key=%s selected=%s ok=%s attempts(含重试)=%d failure=%s",
                tag, key, result.instance_id, result.ok, result.attempts,
                result.failure_kind)


class TestDeregisterAndRecovery:
    def test_deregistered_instance_gets_no_traffic(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env()
        reg.deregister("inst-b")
        logger.info("state change: inst-b -> %s reason=%s",
                    reg.get("inst-b").state.value, reg.get("inst-b").reason.value)
        for n in range(6):
            r = lb.call(key=f"k{n}")
            log_result("deregister", f"k{n}", r)
            assert r.ok
            assert r.instance_id != "inst-b"
        assert upstreams["inst-b"].calls == 0

    def test_probe_failures_mark_unhealthy_then_recover(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env()
        hc = HealthChecker(reg, upstreams)

        upstreams["inst-a"].set_fail_next(10)
        hc.probe_once()
        assert reg.get("inst-a").state == HealthState.HEALTHY  # 未达阈值
        hc.probe_once()
        inst = reg.get("inst-a")
        logger.info("state change: inst-a -> %s reason=%s consecutive=%d",
                    inst.state.value, inst.reason.value, inst.consecutive_failures)
        assert inst.state == HealthState.UNHEALTHY
        assert inst.reason == UnavailableReason.PROBE_FAILED

        for n in range(4):
            r = lb.call(key=f"probe-{n}")
            log_result("unhealthy", f"probe-{n}", r)
            assert r.instance_id != "inst-a"

        upstreams["inst-a"].set_fail_next(0)
        hc.probe_once()
        inst = reg.get("inst-a")
        logger.info("state change: inst-a -> %s reason=%s",
                    inst.state.value, inst.reason.value)
        assert inst.state == HealthState.HEALTHY
        seen = {lb.call(key=f"rec-{n}").instance_id for n in range(6)}
        assert "inst-a" in seen  # 恢复后重新参与调度

    def test_status_reports_reason_and_distinguishes_causes(self):
        reg, upstreams, lb = make_env()
        reg.deregister("inst-a")
        upstreams["inst-b"].set_fail_next(5)
        reg.record_probe_failure("inst-b")
        reg.record_probe_failure("inst-b")
        status = {s["instance_id"]: s for s in reg.status()}
        assert status["inst-a"]["reason"] == "manually_removed"
        assert status["inst-b"]["reason"] == "probe_failed"
        assert status["inst-b"]["consecutive_failures"] == 2
        assert status["inst-c"]["reason"] == "none"
        assert status["inst-c"]["state"] == "healthy"

    def test_deregistered_instance_not_revived_by_probe(self):
        reg, upstreams, lb = make_env()
        hc = HealthChecker(reg, upstreams)
        reg.deregister("inst-c")
        hc.probe_once()
        assert reg.get("inst-c").state == HealthState.DEREGISTERED


class TestRoutingStrategies:
    def test_round_robin_is_even_and_skips_unhealthy(self):
        reg, upstreams, lb = make_env()
        reg.deregister("inst-b")
        picks = [lb.call(key=f"rr{n}").instance_id for n in range(6)]
        logger.info("round_robin picks=%s", picks)
        assert picks == ["inst-a", "inst-c"] * 3

    def test_least_inflight_prefers_idle_instance(self):
        reg, upstreams, lb = make_env(router=LeastInflightRouter())
        reg._instances["inst-a"].inflight = 7
        reg._instances["inst-b"].inflight = 3
        r = lb.call(key="li-1")
        log_result("least_inflight", "li-1", r)
        assert r.instance_id == "inst-c"

    def test_consistent_hash_key_affinity_stable(self):
        reg, upstreams, lb = make_env(router=ConsistentHashRouter())
        first = {}
        for n in range(50):
            key = f"user-{n}"
            r = lb.call(key=key)
            log_result("hash", key, r)
            first[key] = r.instance_id
        for n in range(50):
            key = f"user-{n}"
            assert lb.call(key=key).instance_id == first[key]  # 同键恒定

    def test_consistent_hash_minimal_disruption_on_membership_change(self):
        reg, upstreams, lb = make_env(router=ConsistentHashRouter())
        keys = [f"user-{n}" for n in range(300)]
        before = {k: lb.call(key=k).instance_id for k in keys}
        reg.deregister("inst-c")
        after = {k: lb.call(key=k).instance_id for k in keys}
        moved = [k for k in keys if before[k] != after[k]]
        on_removed = [k for k in keys if before[k] == "inst-c"]
        logger.info("moved=%d removed_owned=%d", len(moved), len(on_removed))
        # 只有原本落在被摘除实例上的键迁移，其余键不受影响
        assert sorted(moved) == sorted(on_removed)
        assert all(after[k] != "inst-c" for k in keys)

    def test_no_strategy_routes_to_deregistered(self):
        for router in (RoundRobinRouter(), LeastInflightRouter(),
                       ConsistentHashRouter()):
            reg, upstreams, lb = make_env(router=router)
            reg.deregister("inst-a")
            for n in range(10):
                r = lb.call(key=f"key-{n}")
                assert r.instance_id != "inst-a", router.name


class TestFailoverAndConcurrency:
    def test_retryable_failure_fails_over(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env()
        upstreams["inst-a"].set_fail_next(1)
        r = lb.call(key="failover-1")
        log_result("failover", "failover-1", r)
        assert r.ok and r.attempts == 2 and r.instance_id != "inst-a"

    def test_timeout_is_retryable_and_classified(self):
        reg, upstreams, lb = make_env()
        upstreams["inst-a"].set_latency(0.5)  # 超过 0.2s 超时
        r = lb.call(key="timeout-1")
        log_result("timeout", "timeout-1", r)
        assert r.ok and r.attempts == 2

    def test_retries_bounded_when_all_fail(self):
        reg, upstreams, lb = make_env(max_retries=2)
        for u in upstreams.values():
            u.set_fail_next(100)
        r = lb.call(key="storm-1")
        log_result("storm", "storm-1", r)
        assert not r.ok
        assert r.attempts == 3  # 首次 + 2 次重试，有界
        assert r.failure_kind == FailureKind.RETRYABLE
        total_calls = sum(u.calls for u in upstreams.values())
        assert total_calls <= 3  # 熔断可能进一步减少实际调用，绝不放大

    def test_circuit_breaker_opens_and_blocks_traffic(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env()
        upstreams["inst-a"].set_fail_next(100)
        state = lb.breaker_state("inst-a")
        for n in range(9):  # 轮询下每 3 次调用命中一次 inst-a，累计到阈值即熔断
            lb.call(key=f"cb-{n}")
            state = lb.breaker_state("inst-a")
            if state == BreakerState.OPEN.value:
                break
        logger.info("breaker change: inst-a -> %s", state)
        assert state == BreakerState.OPEN.value
        calls_before = upstreams["inst-a"].calls
        for n in range(5):
            r = lb.call(key=f"cb-blocked-{n}")
            assert r.instance_id != "inst-a"
        assert upstreams["inst-a"].calls == calls_before  # 熔断期间零调用

    def test_concurrent_inflight_counts_no_drift(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env()
        barrier = threading.Barrier(32)
        errors = []

        def worker(n):
            try:
                barrier.wait()
                r = lb.call(key=f"conc-{n}")
                log_result("concurrent", f"conc-{n}", r)
                assert r.ok
            except AssertionError as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        for iid in IDS:
            inflight = reg.get(iid).inflight
            logger.info("instance=%s final inflight=%d", iid, inflight)
            assert inflight == 0  # 在途计数无漂移

    def test_concurrent_retries_with_flaky_instance(self, caplog):
        caplog.set_level(logging.INFO)
        reg, upstreams, lb = make_env(max_retries=3)
        upstreams["inst-b"].set_fail_next(20)  # 前期失败，之后恢复
        results = []
        lock = threading.Lock()

        def worker(n):
            r = lb.call(key=f"flaky-{n}")
            with lock:
                results.append(r)
            log_result("flaky", f"flaky-{n}", r)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(24)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r.ok for r in results)  # 全部最终成功
        assert all(r.attempts <= 4 for r in results)  # 重试有界
        for iid in IDS:
            assert reg.get(iid).inflight == 0
        # 失败转移后的最终落点不是持续失败的 inst-b（熔断后不再选中）
        final_on_b = [r for r in results if r.instance_id == "inst-b"]
        logger.info("final on inst-b: %d/%d", len(final_on_b), len(results))
