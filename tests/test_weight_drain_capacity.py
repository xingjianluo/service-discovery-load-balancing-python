"""权重分配、优雅排水与并发容量保护的验证测试。"""
from __future__ import annotations

import logging
import threading
import time
from collections import Counter

import pytest

from sd.balancer import LoadBalancer
from sd.health import HealthChecker
from sd.models import HealthState, Instance, UnavailableReason
from sd.registry import ServiceRegistry
from sd.router import ConsistentHashRouter, LeastInflightRouter, RoundRobinRouter
from sd.simulator import SimulatedUpstream

logger = logging.getLogger("test.wdc")

IDS = ["inst-a", "inst-b", "inst-c"]


def make_env(router=None, max_retries=2, failure_threshold=2,
             call_timeout=0.2, **lb_kwargs):
    reg = ServiceRegistry(failure_threshold=failure_threshold)
    upstreams = {}
    for n, iid in enumerate(IDS):
        reg.register(Instance(iid, "127.0.0.1", 9001 + n))
        upstreams[iid] = SimulatedUpstream(iid)
    lb = LoadBalancer(reg, router or RoundRobinRouter(), upstreams,
                      max_retries=max_retries, call_timeout=call_timeout,
                      breaker_threshold=3, breaker_cooldown=0.1, **lb_kwargs)
    return reg, upstreams, lb


class TestWeightedRouting:
    def test_weighted_round_robin_distribution(self):
        reg, _upstreams, lb = make_env()
        reg.set_weight("inst-c", 2)  # 1:1:2
        counts = Counter(lb.call(key=f"w{n}").instance_id for n in range(40))
        logger.info("weighted rr counts=%s", dict(counts))
        assert counts["inst-a"] == 10
        assert counts["inst-b"] == 10
        assert counts["inst-c"] == 20

    def test_weight_hot_adjust_shifts_traffic_without_restart(self):
        reg, _upstreams, lb = make_env()
        before = Counter(lb.call(key=f"h{n}").instance_id for n in range(30))
        assert before["inst-a"] == before["inst-b"] == before["inst-c"] == 10

        reg.set_weight("inst-c", 3)  # 热调整为 1:1:3，不重启
        after = Counter(lb.call(key=f"h2-{n}").instance_id for n in range(50))
        logger.info("after hot adjust counts=%s", dict(after))
        assert after["inst-c"] == 30
        assert after["inst-a"] == after["inst-b"] == 10

    def test_weight_zero_stops_new_traffic_but_keeps_inflight(self):
        reg, _upstreams, lb = make_env()
        assert reg.acquire("inst-b")  # 模拟一个在途请求
        reg.set_weight("inst-b", 0)
        inst = reg.get("inst-b")
        assert not inst.is_available()
        assert inst.reason == UnavailableReason.WEIGHT_ZERO
        for n in range(6):
            r = lb.call(key=f"wz-{n}")
            assert r.ok and r.instance_id != "inst-b"
        assert inst.inflight == 1  # 在途不受影响
        reg.release("inst-b")      # 在途正常归还
        assert reg.get("inst-b").inflight == 0
        # 权重调回后恢复参与调度
        reg.set_weight("inst-b", 1)
        assert reg.get("inst-b").reason == UnavailableReason.NONE
        seen = {lb.call(key=f"back-{n}").instance_id for n in range(6)}
        assert "inst-b" in seen

    def test_weighted_least_inflight_uses_ratio(self):
        reg, _upstreams, lb = make_env(router=LeastInflightRouter())
        reg.set_weight("inst-a", 4)
        reg._instances["inst-a"].inflight = 2  # 比率 2/4 = 0.5
        reg._instances["inst-b"].inflight = 1  # 比率 1/1 = 1.0
        reg._instances["inst-c"].inflight = 1  # 比率 1/1 = 1.0
        r = lb.call(key="wli-1")
        logger.info("weighted least_inflight -> %s", r.instance_id)
        assert r.instance_id == "inst-a"  # 比率最小，尽管绝对在途更高

    def test_weighted_consistent_hash_affinity_and_proportion(self):
        reg, _upstreams, lb = make_env(router=ConsistentHashRouter())
        reg.set_weight("inst-c", 3)
        keys = [f"user-{n}" for n in range(300)]
        first = {k: lb.call(key=k).instance_id for k in keys}
        counts = Counter(first.values())
        logger.info("weighted hash counts=%s", dict(counts))
        assert counts["inst-c"] > counts["inst-a"]
        assert counts["inst-c"] > counts["inst-b"]
        assert counts["inst-c"] / len(keys) > 0.4  # 大致按权重比例
        for k in keys:  # 同键恒定落点
            assert lb.call(key=k).instance_id == first[k]

    def test_hash_weight_zero_excluded_and_rejoin_minimal_migration(self):
        reg, _upstreams, lb = make_env(router=ConsistentHashRouter())
        reg.set_weight("inst-c", 0)
        keys = [f"user-{n}" for n in range(300)]
        before = {k: lb.call(key=k).instance_id for k in keys}
        assert "inst-c" not in before.values()

        reg.set_weight("inst-c", 1)  # 恢复权重：只有迁往 c 的键发生变化
        after = {k: lb.call(key=k).instance_id for k in keys}
        moved = [k for k in keys if before[k] != after[k]]
        logger.info("rejoin moved=%d/%d", len(moved), len(keys))
        assert all(after[k] == "inst-c" for k in moved)
        assert 0 < len(moved) < len(keys)


class TestGracefulDrain:
    def test_drain_completes_inflight_then_finalizes(self):
        reg, upstreams, lb = make_env(call_timeout=1.0)
        upstreams["inst-a"].set_latency(0.3)
        result = {}

        def slow_call():
            result["r"] = lb.call(key="drain-1")  # 首轮轮询必中 inst-a

        t = threading.Thread(target=slow_call)
        t.start()
        time.sleep(0.1)
        assert reg.get("inst-a").inflight == 1

        assert reg.begin_drain("inst-a", timeout=30.0)
        inst = reg.get("inst-a")
        assert inst.state == HealthState.DRAINING
        assert inst.reason == UnavailableReason.DRAINING

        for n in range(4):  # 排水期间新请求不再进入 inst-a
            r = lb.call(key=f"drain-new-{n}")
            assert r.ok and r.instance_id != "inst-a"

        t.join()
        assert result["r"].ok and result["r"].instance_id == "inst-a"  # 在途正常跑完
        inst = reg.get("inst-a")
        logger.info("drain finalized: state=%s reason=%s", inst.state.value,
                    inst.reason.value)
        assert inst.inflight == 0
        assert inst.state == HealthState.DEREGISTERED  # 在途清零后落地
        assert inst.reason == UnavailableReason.DRAINED  # 与排水中状态可区分

    def test_drain_timeout_finalizes_with_inflight(self):
        reg, upstreams, lb = make_env(call_timeout=1.0)
        upstreams["inst-a"].set_latency(0.4)
        done = threading.Event()

        def slow_call():
            lb.call(key="drain-timeout")
            done.set()

        t = threading.Thread(target=slow_call)
        t.start()
        time.sleep(0.1)
        reg.begin_drain("inst-a", timeout=0.15)  # 时限短于在途耗时
        time.sleep(0.2)
        finalized = reg.finalize_drains()
        assert finalized == ["inst-a"]
        inst = reg.get("inst-a")
        assert inst.state == HealthState.DEREGISTERED
        assert inst.reason == UnavailableReason.DRAINED
        assert inst.inflight == 1  # 超时落地时在途尚未结束
        t.join()
        assert done.is_set()
        assert reg.get("inst-a").inflight == 0  # 归还后计数归零，不为负

    def test_drain_is_idempotent_and_concurrency_safe(self):
        reg, upstreams, lb = make_env(call_timeout=1.0)
        upstreams["inst-a"].set_latency(0.3)
        t = threading.Thread(target=lambda: lb.call(key="drain-conc"))
        t.start()
        time.sleep(0.05)
        assert reg.begin_drain("inst-a", timeout=10.0)
        deadline = reg.get("inst-a").drain_deadline

        results = []
        threads = [threading.Thread(
            target=lambda: results.append(reg.begin_drain("inst-a", timeout=0.01)))
            for _ in range(10)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert all(results)  # 重复/并发触发都成功但幂等
        inst = reg.get("inst-a")
        assert inst.state == HealthState.DRAINING  # 不会被提前摘掉
        assert inst.drain_deadline == deadline     # 截止时间不被重置
        assert inst.inflight == 1                  # 在途计数未算错

        t.join()
        assert reg.get("inst-a").inflight == 0
        assert reg.get("inst-a").state == HealthState.DEREGISTERED
        assert reg.finalize_drains() == []  # 已落地，不会重复落地

    def test_drain_unknown_or_deregistered_instance_rejected(self):
        reg, _upstreams, _lb = make_env()
        assert not reg.begin_drain("ghost")
        reg.deregister("inst-b")
        assert not reg.begin_drain("inst-b")

    def test_probe_does_not_revive_draining_instance(self):
        reg, upstreams, _lb = make_env()
        hc = HealthChecker(reg, upstreams)
        reg.begin_drain("inst-a", timeout=30.0)  # 在途为 0 会立即落地
        # 重新注册一个再排水但持有在途，验证探测不复活排水中实例
        reg.register(Instance("inst-a", "127.0.0.1", 9001))
        assert reg.acquire("inst-a")
        reg.begin_drain("inst-a", timeout=30.0)
        hc.probe_once()
        assert reg.get("inst-a").state == HealthState.DRAINING

    def test_status_reports_weight_and_drain_progress(self):
        reg, _upstreams, _lb = make_env()
        reg.set_weight("inst-b", 5)
        reg.set_max_inflight("inst-b", 7)
        reg.begin_drain("inst-c", timeout=30.0)
        status = {s["instance_id"]: s for s in reg.status()}
        assert status["inst-b"]["weight"] == 5
        assert status["inst-b"]["max_inflight"] == 7
        assert status["inst-c"]["state"] == "deregistered"  # 无在途，立即落地
        assert status["inst-c"]["reason"] == "drained"
        assert status["inst-a"]["drain_remaining"] is None


class TestCapacityLimit:
    def test_full_instance_is_bypassed(self):
        reg, upstreams, lb = make_env(call_timeout=1.0)
        reg.set_max_inflight("inst-a", 1)
        upstreams["inst-a"].set_latency(0.3)
        t = threading.Thread(target=lambda: lb.call(key="cap-hold"))
        t.start()
        time.sleep(0.1)
        assert reg.get("inst-a").inflight == 1  # inst-a 已满
        for n in range(4):
            r = lb.call(key=f"cap-{n}")
            assert r.ok and r.instance_id != "inst-a"  # 分流到有余量的实例
        t.join()

    def test_all_at_capacity_fails_fast_with_reason(self):
        reg, upstreams, lb = make_env(call_timeout=1.0)
        reg.deregister("inst-b")
        reg.deregister("inst-c")
        reg.set_max_inflight("inst-a", 1)
        upstreams["inst-a"].set_latency(0.3)
        t = threading.Thread(target=lambda: lb.call(key="cap-hold"))
        t.start()
        time.sleep(0.1)
        r = lb.call(key="cap-full")
        logger.info("all at capacity: ok=%s detail=%s", r.ok, r.detail)
        assert not r.ok
        assert r.attempts == 1  # 尽快失败，不干等
        assert "at capacity" in r.detail
        t.join()

    def test_no_oversubscription_under_concurrency(self):
        reg, upstreams, lb = make_env()
        for iid in IDS:
            reg.set_max_inflight(iid, 2)
            upstreams[iid].set_latency(0.05)
        max_seen = {iid: 0 for iid in IDS}
        stop = threading.Event()

        def monitor():
            while not stop.is_set():
                for iid in IDS:
                    max_seen[iid] = max(max_seen[iid], reg.get(iid).inflight)
                time.sleep(0.002)

        mon = threading.Thread(target=monitor)
        mon.start()
        results = []
        lock = threading.Lock()

        def worker(n):
            r = lb.call(key=f"oversub-{n}")
            with lock:
                results.append(r)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(32)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        stop.set()
        mon.join()
        logger.info("max inflight seen=%s", max_seen)
        assert all(v <= 2 for v in max_seen.values())  # 绝不超发
        for r in results:  # 要么成功，要么以可解释容量原因失败
            assert r.ok or "at capacity" in r.detail
        for iid in IDS:
            assert reg.get(iid).inflight == 0  # 在途计数无漂移


class TestFailoverSwitchesInstance:
    @pytest.mark.parametrize("router", [
        RoundRobinRouter(), LeastInflightRouter(), ConsistentHashRouter()])
    def test_retryable_failure_moves_to_another_instance(self, router):
        reg, upstreams, lb = make_env(router=router)
        probe = type(router)()
        winner = probe.pick(reg.healthy_instances(), key="fk").instance_id
        upstreams[winner].set_fail_next(1)
        r = lb.call(key="fk")
        logger.info("router=%s first=%s final=%s attempts=%d",
                    router.name, winner, r.instance_id, r.attempts)
        assert r.ok and r.attempts == 2
        assert r.instance_id != winner  # 重试必须换实例

    @pytest.mark.parametrize("router", [
        RoundRobinRouter(), LeastInflightRouter(), ConsistentHashRouter()])
    def test_timeout_moves_to_another_instance(self, router):
        reg, upstreams, lb = make_env(router=router)
        probe = type(router)()
        winner = probe.pick(reg.healthy_instances(), key="ft").instance_id
        upstreams[winner].set_latency(0.5)  # 超过 0.2s 超时
        r = lb.call(key="ft")
        assert r.ok and r.attempts == 2
        assert r.instance_id != winner
