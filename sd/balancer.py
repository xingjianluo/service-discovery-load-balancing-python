"""负载均衡器：组合注册表、择路策略、熔断与有限重试。"""
from __future__ import annotations

import logging
import threading

from .breaker import CircuitBreaker
from .models import CallResult, FailureKind, RouteDecision
from .registry import ServiceRegistry
from .router import Router
from .simulator import SimulatedUpstream

logger = logging.getLogger("sd.balancer")


class LoadBalancer:
    """对外入口：择路 + 调用 + 故障转移（有界重试，避免重试风暴）。

    并发语义：
    - 择路候选快照与 acquire（在途 +1）在注册表锁内完成，杜绝
      “选中后实例已被摘除仍被加计数/转发”的竞态。
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

    def _eligible(self) -> list:
        """健康（含权重>0）、熔断器放行且容量有余量的实例。"""
        return [i for i in self._registry.healthy_instances()
                if i.has_capacity() and self._breaker_for(i.instance_id).allow()]

    def _no_candidate_detail(self) -> str:
        """没有候选时给出可解释的原因。"""
        healthy = self._registry.healthy_instances()
        if healthy and all(not i.has_capacity() for i in healthy):
            return "all instances at capacity (max_inflight reached)"
        return "no eligible instance"

    def call(self, key: str | None = None) -> CallResult:
        last_kind: FailureKind | None = None
        last_detail = "no attempt"
        last_instance: str | None = None
        excluded: set[str] = set()  # 本次调用中已失败/不可用的实例，重试必须换实例

        for attempt in range(1, self._max_retries + 2):  # 首次 + max_retries 次重试
            candidates = [i for i in self._eligible()
                          if i.instance_id not in excluded]
            decision = self._router.pick(candidates, key)
            with self._lock:
                self.decision_log.append(decision)
            if decision.instance_id is None:
                detail = self._no_candidate_detail()
                logger.info("key=%s attempt=%d %s", key, attempt, detail)
                return CallResult(False, None, attempt, FailureKind.RETRYABLE, detail)
            iid = decision.instance_id
            last_instance = iid
            # acquire 与可用性/容量检查原子完成；失败说明并发下实例刚被摘除或打满，换实例重选
            if not self._registry.acquire(iid):
                logger.info("key=%s attempt=%d instance=%s became unavailable, re-pick",
                            key, attempt, iid)
                excluded.add(iid)
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
            excluded.add(iid)  # 可重试失败/超时：下一次尝试必须转移到其他实例
            last_kind, last_detail = kind, detail
            logger.info("key=%s attempt=%d instance=%s failed kind=%s detail=%s",
                        key, attempt, iid, kind.value if kind else "?", detail)
            if kind == FailureKind.NON_RETRYABLE:
                return CallResult(False, iid, attempt, kind, detail)
            # 可重试/超时：进入下一次尝试，转移到其他实例

        return CallResult(False, last_instance, self._max_retries + 1,
                          last_kind, f"retries exhausted: {last_detail}")

    def breaker_state(self, instance_id: str) -> str:
        return self._breaker_for(instance_id).state.value
