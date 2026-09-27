"""服务注册表：注册、摘除、健康状态收敛、权重/容量热调、优雅排水（线程安全）。"""
from __future__ import annotations

import threading
import time

from .models import HealthState, Instance, UnavailableReason

# 探测成功不应复活的终态/运维态
_PROBE_FROZEN_STATES = (
    HealthState.DEREGISTERED,
    HealthState.DRAINING,
    HealthState.DRAINED,
)


class ServiceRegistry:
    """维护实例集合及其健康状态，所有变更都在锁内完成。"""

    def __init__(self, failure_threshold: int = 3) -> None:
        self._lock = threading.RLock()
        self._instances: dict[str, Instance] = {}
        self._failure_threshold = failure_threshold

    def register(
        self,
        instance: Instance,
        weight: int | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        with self._lock:
            if weight is not None:
                if weight < 0:
                    raise ValueError("weight must be >= 0")
                instance.weight = weight
            if max_concurrency is not None:
                if max_concurrency < 0:
                    raise ValueError("max_concurrency must be >= 0")
                instance.max_concurrency = max_concurrency
            # （重新）注册视为重新上线：清空健康/排水/在途等所有运行态
            instance.state = HealthState.HEALTHY
            instance.reason = UnavailableReason.NONE
            instance.consecutive_failures = 0
            instance.inflight = 0
            instance.drain_deadline = None
            self._instances[instance.instance_id] = instance

    def deregister(self, instance_id: str) -> bool:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False
            inst.state = HealthState.DEREGISTERED
            inst.reason = UnavailableReason.MANUALLY_REMOVED
            inst.drain_deadline = None
            return True

    def set_weight(self, instance_id: str, weight: int) -> bool:
        """热调整权重（0 立即不接新请求，不影响在途请求）。"""
        if weight < 0:
            raise ValueError("weight must be >= 0")
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False
            inst.weight = weight
            return True

    def set_max_concurrency(self, instance_id: str, max_concurrency: int) -> bool:
        """热调整实例级并发容量上限（0 表示不限）。"""
        if max_concurrency < 0:
            raise ValueError("max_concurrency must be >= 0")
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False
            inst.max_concurrency = max_concurrency
            return True

    def begin_drain(self, instance_id: str, drain_timeout: float) -> bool:
        """让实例进入排水：此后不接新请求，在途请求跑完或超时后落地。

        幂等：已在排水中的实例不会被重复触发刷新时限（避免提前/延后落地），
        在途计数也不受影响。返回 False 表示未发起新的排水。
        """
        if drain_timeout < 0:
            raise ValueError("drain_timeout must be >= 0")
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False
            if inst.state == HealthState.DRAINING:
                return False  # 重复/并发触发：保留原时限，什么都不做
            if inst.state in (HealthState.DEREGISTERED, HealthState.DRAINED):
                return False
            inst.state = HealthState.DRAINING
            inst.reason = UnavailableReason.DRAINING
            inst.drain_deadline = time.monotonic() + drain_timeout
            # 在途为零或时限为零：立即落地
            if inst.inflight == 0 or drain_timeout == 0:
                self._settle_drain_locked(inst)
            return True

    def _settle_drain_locked(self, inst: Instance, now: float | None = None) -> None:
        """排水中的实例满足“在途清零”或“超时”则落地为 DRAINED。调用方持锁。"""
        if inst.state != HealthState.DRAINING:
            return
        now = time.monotonic() if now is None else now
        deadline = inst.drain_deadline
        if inst.inflight == 0 or (deadline is not None and now >= deadline):
            inst.state = HealthState.DRAINED
            inst.reason = UnavailableReason.DRAINED

    def settle_drains(self) -> None:
        """推进所有排水实例的落地判定（在途清零或超时）。"""
        now = time.monotonic()
        with self._lock:
            for inst in self._instances.values():
                self._settle_drain_locked(inst, now)

    def get(self, instance_id: str) -> Instance | None:
        with self._lock:
            return self._instances.get(instance_id)

    def all_instances(self) -> list[Instance]:
        with self._lock:
            return list(self._instances.values())

    def healthy_instances(self) -> list[Instance]:
        with self._lock:
            return [i for i in self._instances.values() if i.is_available()]

    def scheduling_reasons(self) -> dict[str, str]:
        """所有实例此刻“是否参与调度及具体原因”的快照。"""
        with self._lock:
            return {
                i.instance_id: i.scheduling_reason().value
                for i in self._instances.values()
            }

    def record_probe_success(self, instance_id: str) -> None:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state in _PROBE_FROZEN_STATES:
                return  # 摘除/排水中的实例不因探测结果改变状态
            inst.consecutive_failures = 0
            inst.state = HealthState.HEALTHY
            inst.reason = UnavailableReason.NONE

    def record_probe_failure(self, instance_id: str) -> None:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state in _PROBE_FROZEN_STATES:
                return
            inst.consecutive_failures += 1
            if inst.consecutive_failures >= self._failure_threshold:
                inst.state = HealthState.UNHEALTHY
                inst.reason = UnavailableReason.PROBE_FAILED

    def acquire(self, instance_id: str) -> bool:
        """在途计数 +1，成功返回 True（兼容旧接口）。"""
        ok, _reason = self.acquire_with_reason(instance_id)
        return ok

    def acquire_with_reason(
        self, instance_id: str,
    ) -> tuple[bool, UnavailableReason | None]:
        """在途计数 +1，失败时返回当时最具体的不可用原因。

        仅当实例健康、权重 > 0、未达并发容量上限时才放行；
        与择路在同一锁体系内完成检查，杜绝超发与“选中后被摘走”的竞态。
        """
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False, UnavailableReason.MANUALLY_REMOVED
            if not inst.is_available():
                return False, inst.scheduling_reason()
            if inst.weight <= 0:
                return False, UnavailableReason.WEIGHT_ZERO
            if inst.at_capacity():
                return False, UnavailableReason.AT_CAPACITY
            inst.inflight += 1
            return True, None


    def release(self, instance_id: str) -> None:
        """在途计数 -1，不允许出现负数；排水中的实例在途清零即落地。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.inflight <= 0:
                return
            inst.inflight -= 1
            if inst.state == HealthState.DRAINING and inst.inflight == 0:
                self._settle_drain_locked(inst)

    def status(self) -> list[dict]:
        """返回每个实例的健康判定依据、权重容量与当前调度状态。"""
        now = time.monotonic()
        with self._lock:
            rows = []
            for i in self._instances.values():
                drain_remaining: float | None = None
                if i.state == HealthState.DRAINING and i.drain_deadline is not None:
                    drain_remaining = max(0.0, i.drain_deadline - now)
                rows.append({
                    "instance_id": i.instance_id,
                    "address": i.address,
                    "state": i.state.value,
                    "reason": i.reason.value,
                    "scheduling_reason": i.scheduling_reason().value,
                    "consecutive_failures": i.consecutive_failures,
                    "inflight": i.inflight,
                    "weight": i.weight,
                    "max_concurrency": i.max_concurrency,
                    "drain_remaining_seconds": drain_remaining,
                    "failure_threshold": self._failure_threshold,
                })
            return rows
