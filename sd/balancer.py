"""负载均衡器：组合注册表、择路策略、熔断与有限重试。"""
from __future__ import annotations

import logging
import threading

from .breaker import BreakerState, CircuitBreaker
from .models import CallResult, FailureKind, RouteDecision, UnavailableReason
from .registry import ServiceRegistry
from .router import Router
from .simulator import SimulatedUpstream

logger = logging.getLogger("sd.balancer")


class LoadBalancer:
    """对外入口：择路 + 调用 + 故障转移（有界重试，避免重试风暴）。

    并发语义：
    - 择路候选快照与 acquire（在途 +1）在注册表锁内完成，杜绝
      “选中后实例已被摘除/排水/到容量仍被加计数/转发”的竞态。
    - 候选已排除权重 0、排水中、并发到上限及熔断打开的实例。
    - 一次调用内已试过的实例不会被再次选中，故障转移一定换实例。
    - 熔断器状态迁移各自持锁，allow/on_failure 配对不会漂移。
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        router: Router,
        upstreams: dict[str, SimulatedUpstream],
        max_retries: int = 2,
        call_timeout: float = 1.0,
        breaker_threshold: int = 3,
        breaker_cooldown: float = 5.0,
    ) -> None:
        self._registry = registry
        self._router = router
        self._upstreams = upstreams
        self._max_retries = max_retries
        self._timeout = call_timeout
        self._breaker_threshold = breaker_threshold
        self._breaker_cooldown = breaker_cooldown
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
        self.decision_log: list[RouteDecision] = []

    def _breaker_for(self, instance_id: str) -> CircuitBreaker:
        with self._lock:
            br = self._breakers.get(instance_id)
            if br is None:
                br = CircuitBreaker(self._breaker_threshold, self._breaker_cooldown)
                self._breakers[instance_id] = br
            return br

    def _eligible(self, tried: frozenset[str]) -> tuple[list, bool]:
        """返回 (可择路候选, 是否因并发容量被过滤)。

        候选条件：注册表认为可调度（健康/权重>0/未排水）、熔断器放行、
        未达实例并发上限、本次调用还没试过（故障转移不复试同一实例）。
        """
        all_healthy = self._registry.healthy_instances()
        allowed: list = []
        capacity_filtered = False
        for inst in all_healthy:
            if inst.instance_id in tried:
                continue  # 本次调用已试过，故障转移必须换实例
            if not self._breaker_for(inst.instance_id).allow():
                continue
            if inst.weight <= 0:
                continue
            if inst.at_capacity():
                capacity_filtered = True
                continue
            allowed.append(inst)
        return allowed, capacity_filtered

    def call(self, key: str | None = None) -> CallResult:
        last_kind: FailureKind | None = None
        last_detail = "no attempt"
        last_instance: str | None = None
        tried: set[str] = set()
        acquire_capacity_seen = False  # 竞态窗口里是否撞到过并发到顶

        for attempt in range(1, self._max_retries + 2):  # 首次 + max_retries 次重试
            self._registry.settle_drains()  # 推进排水超时落地
            candidates, capacity_filtered = self._eligible(frozenset(tried))
            decision = self._router.pick(
                candidates, key, exclude=frozenset(tried))
            with self._lock:
                self.decision_log.append(decision)
            if decision.instance_id is None:
                # 区分“全体并发到顶”与“没有可调度实例”，给出可解释原因
                if capacity_filtered or acquire_capacity_seen:
                    logger.info("key=%s attempt=%d all instances at capacity",
                                key, attempt)
                    return CallResult(False, None, attempt,
                                      FailureKind.CAPACITY_EXHAUSTED,
                                      "all instances at concurrency capacity")
                logger.info("key=%s attempt=%d no eligible instance", key, attempt)
                return CallResult(False, None, attempt, FailureKind.RETRYABLE,
                                  "no eligible instance")
            iid = decision.instance_id
            last_instance = iid
            tried.add(iid)  # 无论成功与否，本次调用不再复试同一实例
            # acquire 与可用性/容量检查原子完成；失败说明并发下状态刚变化，重选
            acquired, acquire_reason = self._registry.acquire_with_reason(iid)
            if not acquired:
                if acquire_reason == UnavailableReason.AT_CAPACITY:
                    acquire_capacity_seen = True
                logger.info("key=%s attempt=%d instance=%s not acquirable (%s), re-pick",
                            key, attempt, iid,
                            acquire_reason.value if acquire_reason else "?")
                continue
            try:
                upstream = self._upstreams[iid]
                ok, kind, detail = upstream.call(self._timeout)
            finally:
                self._registry.release(iid)

            breaker = self._breaker_for(iid)
            if ok:
                breaker.on_success()
                logger.info("key=%s attempt=%d instance=%s ok", key, attempt, iid)
                return CallResult(True, iid, attempt, None, detail)

            breaker.on_failure()
            last_kind, last_detail = kind, detail
            logger.info("key=%s attempt=%d instance=%s failed kind=%s detail=%s",
                        key, attempt, iid, kind.value if kind else "?", detail)
            if kind == FailureKind.NON_RETRYABLE:
                return CallResult(False, iid, attempt, kind, detail)
            # 可重试/超时：进入下一次尝试，候选已排除试过的实例

        return CallResult(False, last_instance, self._max_retries + 1,
                          last_kind, f"retries exhausted: {last_detail}")

    def breaker_state(self, instance_id: str) -> str:
        return self._breaker_for(instance_id).state.value
