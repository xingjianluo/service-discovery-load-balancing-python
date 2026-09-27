"""灰度权重、优雅排水、实例并发容量的本地验证测试。

覆盖：
- 权重分配与热调整（三种择路都把权重纳入考量）；
- 排水：在途请求跑完、超时落地、状态可区分、重复/并发触发幂等；
- 并发容量：不超发、全到顶快速失败、计数无漂移；
- 故障转移不回归：三种择路都换到别的实例，不反复重试坏实例。
"""
from __future__ import annotations

import logging
import threading

from sd.balancer import LoadBalancer
from sd.models import FailureKind, HealthState, Instance, UnavailableReason
from sd.registry import ServiceRegistry
from sd.router import ConsistentHashRouter, LeastInflightRouter, RoundRobinRouter
from sd.simulator import SimulatedUpstream

logger = logging.getLogger("test.sd.gray")

IDS = ["inst-a", "inst-b", "inst-c"]


def make_env(router=None, max_retries=2, failure_threshold=2, **overrides):
    reg = ServiceRegistry(failure_threshold=failure_threshold)
    upstreams = {}
    for n, iid in enumerate(IDS):
        inst = Instance(iid, "127.0.0.1", 9001 + n)
        weight = overrides.get("weights", {}).get(iid, 1)
        cap = overrides.get("concurrencies", {}).get(iid, 0)
        reg.register(inst, weight=weight, max_concurrency=cap)
        upstreams[iid] = SimulatedUpstream(iid)
    lb = LoadBalancer(reg, router or RoundRobinRouter(), upstreams,
                      max_retries=max_retries, call_timeout=0.5,
                      breaker_threshold=3, breaker_cooldown=0.1)
    return reg, upstreams, lb


def counts_of(results):
    out: dict[str, int] = {}
    for iid in results:
        out[iid] = out.get(iid, 0) + 1
    return out


class TestWeightedRouting:
    def test_round_robin_splits_by_weight(self):
        reg, ups, lb = make_env(weights={"inst-a": 3, "inst-b": 1, "inst-c": 1})
        picks = [lb.call(key=f"w{n}").instance_id for n in range(100)]
        counts = counts_of(picks)
        logger.info("weighted rr counts=%s", counts)
        assert counts == {"inst-a": 60, "inst-b": 20, "inst-c": 20}

    def test_default_weight_keeps_even_distribution(self):
        # 不显式指定权重时，行为与改造前一致
        reg, ups, lb = make_env()
        picks = [lb.call(key=f"d{n}").instance_id for n in range(9)]
        assert picks == ["inst-a", "inst-b", "inst-c"] * 3

    def test_weight_zero_gets_no_new_traffic_but_inflight_kept(self):
        reg, ups, lb = make_env()
        # 模拟 inst-b 已有一个在途请求
        assert reg.acquire("inst-b")
        assert reg.get("inst-b").inflight == 1
        reg.set_weight("inst-b", 0)
        assert reg.get("inst-b").scheduling_reason() == UnavailableReason.WEIGHT_ZERO
        for n in range(12):
            r = lb.call(key=f"z{n}")
            assert r.ok and r.instance_id != "inst-b"
        assert ups["inst-b"].calls == 0
        # 在途计数不受权重置 0 影响，正常归还
        reg.release("inst-b")
        assert reg.get("inst-b").inflight == 0

    def test_weight_hot_update_reshapes_traffic_smoothly(self):
        reg, ups, lb = make_env()
        before = counts_of(lb.call(key=f"b{n}").instance_id for n in range(9))
        assert before == {"inst-a": 3, "inst-b": 3, "inst-c": 3}
        reg.set_weight("inst-a", 5)  # 热调大，无需重启
        after = counts_of(lb.call(key=f"a{n}").instance_id for n in range(70))
        logger.info("after hot weight a=5 counts=%s", after)
        assert after == {"inst-a": 50, "inst-b": 10, "inst-c": 10}
        # 调回 0：立刻不再接新请求
        reg.set_weight("inst-a", 0)
        for n in range(10):
            assert lb.call(key=f"q{n}").instance_id in {"inst-b", "inst-c"}

    def test_status_reports_weight_and_scheduling_reason(self):
        reg, ups, lb = make_env()
        reg.set_weight("inst-b", 0)
        reg.set_max_concurrency("inst-c", 1)
        reg.acquire("inst-c")  # 打满
        rows = {s["instance_id"]: s for s in reg.status()}
        assert rows["inst-a"]["weight"] == 1
        assert rows["inst-b"]["weight"] == 0
        assert rows["inst-b"]["scheduling_reason"] == "weight_zero"
        assert rows["inst-c"]["scheduling_reason"] == "at_capacity"
        assert rows["inst-c"]["max_concurrency"] == 1
        assert rows["inst-a"]["scheduling_reason"] == "none"
        reg.release("inst-c")

    def test_least_inflight_respects_weight(self):
        reg, ups, lb = make_env(router=LeastInflightRouter())
        reg.get("inst-a").inflight = 2
        reg.get("inst-b").inflight = 1
        reg.get("inst-c").inflight = 1
        # 等权：b、c 占用率(=在途)最低且并列，取 id 小者 b
        assert lb.call(key="li-w-1").instance_id == "inst-b"
        # 在途不变，仅把 a 权重调高：a 占用率 2/10=0.2 最低 -> 选择翻转为 a
        reg.set_weight("inst-a", 10)
        assert lb.call(key="li-w-2").instance_id == "inst-a"
        # 调回等权后退化为纯最少在途：把 b 抬高，c 在途最低 -> c
        reg.set_weight("inst-a", 1)
        reg.get("inst-b").inflight = 5
        assert lb.call(key="li-w-3").instance_id == "inst-c"

    def test_consistent_hash_weighted_ring_and_affinity(self):
        reg, ups, lb = make_env(router=ConsistentHashRouter(),
                                weights={"inst-a": 4, "inst-b": 1, "inst-c": 1})
        keys = [f"u-{n}" for n in range(300)]
        mapping = {k: lb.call(key=k).instance_id for k in keys}
        # 同键恒定
        assert all(lb.call(key=k).instance_id == mapping[k] for k in keys)
        counts = counts_of(mapping.values())
        logger.info("weighted ch counts=%s", counts)
        assert counts["inst-a"] > counts["inst-b"] + counts["inst-c"]  # 高权重占多数
        # 权重 0 的实例不在环上
        reg.set_weight("inst-a", 0)
        assert all(lb.call(key=k).instance_id != "inst-a" for k in keys)


class TestDrain:
    def test_draining_instance_rejects_new_but_inflight_completes(self):
        reg, ups, lb = make_env()  # 默认轮询：第一次调用必落在排序最前的 inst-a
        gate = threading.Event()
        ups["inst-a"].set_gate(gate)  # 只拦截将持有在途的那一个实例
        done = threading.Event()
        holder_result = {}

        def holder():
            holder_result["r"] = lb.call(key="drain-held")
            done.set()

        t = threading.Thread(target=holder)
        t.start()
        # 等在途请求确实落在 inst-a 并挂在闸门上
        for _ in range(200):
            if reg.get("inst-a").inflight == 1:
                break
            threading.Event().wait(0.01)
        held = "inst-a"
        assert reg.get(held).inflight == 1

        assert reg.begin_drain(held, drain_timeout=30.0)
        assert reg.get(held).state == HealthState.DRAINING
        assert reg.get(held).scheduling_reason() == UnavailableReason.DRAINING
        # 排水期间新请求一律不落到该实例（落到 b/c，闸门不拦截它们）
        for n in range(10):
            assert lb.call(key=f"new{n}").instance_id != held
        assert reg.get(held).inflight == 1  # 在途仍在

        # 放行为在途请求，它应正常成功，且计数归还、实例落地
        gate.set()
        t.join(timeout=2)
        assert done.is_set()
        assert holder_result["r"].ok and holder_result["r"].instance_id == held
        assert reg.get(held).inflight == 0
        assert reg.get(held).state == HealthState.DRAINED
        assert reg.get(held).reason == UnavailableReason.DRAINED

    def test_drain_times_out_and_settles(self):
        reg, ups, lb = make_env()
        reg.acquire("inst-a")  # 模拟一个迟迟不结束的在途请求
        reg.begin_drain("inst-a", drain_timeout=0.05)
        assert reg.get("inst-a").state == HealthState.DRAINING
        threading.Event().wait(0.08)
        reg.settle_drains()  # balancer 每次 call 也会推进；这里显式调用
        assert reg.get("inst-a").state == HealthState.DRAINED
        # 排水超时落地后仍不接新请求
        for n in range(5):
            assert lb.call(key=f"t{n}").instance_id != "inst-a"
        # 在途计数保留到真正 release，不被提前清零
        assert reg.get("inst-a").inflight == 1
        reg.release("inst-a")
        assert reg.get("inst-a").inflight == 0
        assert reg.get("inst-a").state == HealthState.DRAINED

    def test_drain_zero_timeout_settles_immediately(self):
        reg, ups, lb = make_env()
        assert reg.begin_drain("inst-c", drain_timeout=0)
        assert reg.get("inst-c").state == HealthState.DRAINED

    def test_concurrent_drain_is_idempotent_and_preserves_inflight(self):
        reg, ups, lb = make_env()
        reg.acquire("inst-b")
        results = []
        barrier = threading.Barrier(8)

        def drainer():
            barrier.wait()
            results.append(reg.begin_drain("inst-b", drain_timeout=30.0))

        threads = [threading.Thread(target=drainer) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 只有第一次触发返回 True，其余幂等返回 False
        assert results.count(True) == 1 and results.count(False) == 7
        inst = reg.get("inst-b")
        assert inst.state == HealthState.DRAINING
        assert inst.inflight == 1  # 在途数没有被算错
        reg.release("inst-b")
        assert inst.state == HealthState.DRAINED

    def test_status_distinguishes_draining_and_drained(self):
        reg, ups, lb = make_env()
        reg.acquire("inst-a")  # 保留在途，使 a 停留在 draining
        reg.begin_drain("inst-a", drain_timeout=30.0)
        reg.acquire("inst-b")
        reg.begin_drain("inst-b", drain_timeout=30.0)
        reg.release("inst-b")  # 在途清零立即落地
        rows = {s["instance_id"]: s for s in reg.status()}
        assert rows["inst-a"]["state"] == "draining"
        assert rows["inst-a"]["scheduling_reason"] == "draining"
        assert rows["inst-a"]["drain_remaining_seconds"] is not None
        assert rows["inst-b"]["state"] == "drained"
        assert rows["inst-b"]["scheduling_reason"] == "drained"
        assert rows["inst-c"]["state"] == "healthy"
        reg.release("inst-a")

    def test_drained_instance_can_come_back_online_by_reregister(self):
        reg, ups, lb = make_env()
        reg.begin_drain("inst-b", drain_timeout=0)
        assert reg.get("inst-b").state == HealthState.DRAINED
        for n in range(4):
            assert lb.call(key=f"off{n}").instance_id != "inst-b"
        # 重新注册即重新上线，权重/容量等运行态复位，重新接流量
        reg.register(Instance("inst-b", "127.0.0.1", 9002), weight=1)
        assert reg.get("inst-b").state == HealthState.HEALTHY
        seen = {lb.call(key=f"on-{n}").instance_id for n in range(9)}
        assert "inst-b" in seen


class TestConcurrencyCapacity:
    def test_calls_failed_when_all_at_capacity_fail_fast(self):
        caps = {"inst-a": 1, "inst-b": 1, "inst-c": 1}
        reg, ups, lb = make_env(concurrencies=caps)
        assert reg.acquire("inst-a")
        assert reg.acquire("inst-b")
        assert reg.acquire("inst-c")  # 三个实例都占满
        for iid in IDS:
            assert reg.get(iid).scheduling_reason() == UnavailableReason.AT_CAPACITY
        r = lb.call(key="cap-full")
        assert not r.ok
        assert r.failure_kind == FailureKind.CAPACITY_EXHAUSTED
        assert r.attempts == 1  # 不干等、不多试，尽快失败
        for iid in IDS:
            reg.release(iid)

    def test_traffic_spills_to_instance_with_headroom(self):
        caps = {"inst-a": 1, "inst-b": 0, "inst-c": 0}
        reg, ups, lb = make_env(concurrencies=caps)
        assert reg.acquire("inst-a")  # a 到上限
        for n in range(8):
            r = lb.call(key=f"spill{n}")
            assert r.ok and r.instance_id != "inst-a"
        reg.release("inst-a")

    def test_no_oversubscription_under_concurrency(self):
        # 每实例容量 2，总计 6；8 个并发请求：恰好 2 个应快速失败，其余成功
        caps = {iid: 2 for iid in IDS}
        reg, ups, lb = make_env(concurrencies=caps)
        gate = threading.Event()
        for u in ups.values():
            u.set_gate(gate)
        barrier = threading.Barrier(8)
        results: list = []
        rlock = threading.Lock()

        def worker(n):
            barrier.wait()
            r = lb.call(key=f"cap-{n}")
            with rlock:
                results.append(r)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        threading.Event().wait(0.2)  # 等成功的 6 个全部挂在闸门上
        # 任何时刻在途都不得超过各实例容量
        for iid in IDS:
            assert reg.get(iid).inflight <= caps[iid]
        gate.set()
        for t in threads:
            t.join(timeout=2)
        oks = [r for r in results if r.ok]
        caps_failed = [r for r in results
                       if r.failure_kind == FailureKind.CAPACITY_EXHAUSTED]
        logger.info("ok=%d capacity_failed=%d", len(oks), len(caps_failed))
        assert len(oks) == 6
        assert len(caps_failed) == 2
        for iid in IDS:
            assert reg.get(iid).inflight == 0  # 计数无漂移

    def test_capacity_hot_update_takes_effect(self):
        reg, ups, lb = make_env()
        reg.set_max_concurrency("inst-a", 1)
        assert reg.acquire("inst-a")
        assert reg.get("inst-a").scheduling_reason() == UnavailableReason.AT_CAPACITY
        # 调大前 a 不可接新流量
        for n in range(4):
            assert lb.call(key=f"c{n}").instance_id != "inst-a"
        reg.release("inst-a")
        reg.set_max_concurrency("inst-a", 0)  # 0 = 取消限制
        seen = {lb.call(key=f"u{n}").instance_id for n in range(6)}
        assert "inst-a" in seen


class TestFailoverNotRegressed:
    def _first_two_picks(self, router, key):
        """用同类型独立路由器推算该策略下的首轮、次轮落点，避免污染 LB 状态。"""
        reg0, _ups0, _lb0 = make_env(router=type(router)())
        healthy = reg0.healthy_instances()
        first = type(router)().pick(healthy, key=key).instance_id
        second = type(router)().pick(
            healthy, key=key, exclude=frozenset([first])).instance_id
        return first, second

    def test_all_strategies_fail_over_to_other_instance(self):
        for router in (RoundRobinRouter(), LeastInflightRouter(),
                       ConsistentHashRouter()):
            reg, ups, lb = make_env(router=router)
            first, second = self._first_two_picks(router, "fo-1")
            third = [i for i in IDS if i not in (first, second)][0]
            ups[first].set_fail_next(1)
            ups[second].set_fail_next(1)
            r = lb.call(key="fo-1")
            logger.info("[%s] first=%s second=%s -> result=%s attempts=%d",
                        router.name, first, second, r.instance_id, r.attempts)
            assert r.ok
            assert r.attempts == 3  # first、second 各试一次失败，third 成功
            assert r.instance_id == third

    def test_consistent_hash_retry_moves_around_ring(self):
        reg, ups, lb = make_env(router=ConsistentHashRouter())
        key = "affinity-key"
        owner, _n = self._first_two_picks(ConsistentHashRouter(), key)
        # 先确认一次正常调用落点
        assert lb.call(key=key).instance_id == owner
        ups[owner].set_fail_next(1)
        r = lb.call(key=key)
        assert r.ok and r.attempts == 2
        assert r.instance_id != owner  # 重试换到环上下一个实例
        # 故障清除后同键仍回原属主，亲和关系不永久漂移
        assert lb.call(key=key).instance_id == owner

    def test_timeout_failover_does_not_repeat_same_instance(self):
        for router in (RoundRobinRouter(), LeastInflightRouter(),
                       ConsistentHashRouter()):
            reg, ups, lb = make_env(router=router, max_retries=2)
            first, _s = self._first_two_picks(router, "to-1")
            ups[first].set_latency(0.6)  # 超过 0.5s 超时
            r = lb.call(key="to-1")
            assert r.ok and r.attempts == 2 and r.instance_id != first
            # 超时实例只被尝试一次，绝不复试同一个
            assert ups[first].calls == 1

    def test_inflight_and_breaker_unaffected_by_weight_changes(self):
        reg, ups, lb = make_env()
        ups["inst-b"].set_fail_next(20)
        threads = []
        for n in range(18):
            threads.append(threading.Thread(
                target=lambda n=n: (
                    reg.set_weight("inst-a", (n % 3)),
                    lb.call(key=f"mix-{n}"),
                    reg.set_weight("inst-a", 1),
                )))
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3)
        for iid in IDS:
            assert reg.get(iid).inflight == 0  # 在途计数零漂移
