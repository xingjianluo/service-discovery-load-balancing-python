"""服务注册表：注册、摘除、权重热调整、优雅排水、容量与在途计数（线程安全）。"""
from __future__ import annotations

import threading
import time

from .models import HealthState, Instance, UnavailableReason


class ServiceRegistry:
    """维护实例集合及其健康状态，所有变更都在锁内完成。"""

    def __init__(self, failure_threshold: int = 3) -> None:
        self._lock = threading.RLock()
        self._instances: dict[str, Instance] = {}
        self._failure_threshold = failure_threshold

    def register(self, instance: Instance) -> None:
        with self._lock:
            instance.state = HealthState.HEALTHY
            instance.reason = (UnavailableReason.WEIGHT_ZERO if instance.weight <= 0
                               else UnavailableReason.NONE)
            instance.consecutive_failures = 0
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
        """热调整权重：立即生效，无需重启。权重 0 停止接新请求但不动在途。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or weight < 0:
                return False
            inst.weight = weight
            if inst.state == HealthState.HEALTHY:
                if weight == 0 and inst.reason == UnavailableReason.NONE:
                    inst.reason = UnavailableReason.WEIGHT_ZERO
                elif weight > 0 and inst.reason == UnavailableReason.WEIGHT_ZERO:
                    inst.reason = UnavailableReason.NONE
            return True

    def set_max_inflight(self, instance_id: str, max_inflight: int) -> bool:
        """热调整并发容量上限（0 表示不限）。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or max_inflight < 0:
                return False
            inst.max_inflight = max_inflight
            return True

    def begin_drain(self, instance_id: str, timeout: float = 30.0) -> bool:
        """让实例进入排水状态：不再接新请求，在途请求继续跑完。

        - 幂等：已在排水中的实例重复触发不会重置截止时间，也不会提前摘除。
        - 在途为 0 时立即落地为 deregistered/drained。
        """
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state == HealthState.DEREGISTERED:
                return False
            if inst.state == HealthState.DRAINING:
                return True  # 幂等：不重置截止时间
            inst.state = HealthState.DRAINING
            inst.reason = UnavailableReason.DRAINING
            inst.drain_deadline = time.monotonic() + timeout
            if inst.inflight == 0:
                self._finalize_drain(inst)
            return True

    def finalize_drains(self) -> list[str]:
        """把在途清零或超过排水时限的实例落地为不可用。返回本次落地的实例 id。"""
        now = time.monotonic()
        finalized: list[str] = []
        with self._lock:
            for inst in self._instances.values():
                if inst.state != HealthState.DRAINING:
                    continue
                if inst.inflight == 0 or (inst.drain_deadline is not None
                                          and now >= inst.drain_deadline):
                    self._finalize_drain(inst)
                    finalized.append(inst.instance_id)
        return finalized

    def _finalize_drain(self, inst: Instance) -> None:
        """落地排水结果（调用方须持锁）。状态与排水中区分开。"""
        inst.state = HealthState.DEREGISTERED
        inst.reason = UnavailableReason.DRAINED
        inst.drain_deadline = None

    def get(self, instance_id: str) -> Instance | None:
        with self._lock:
            return self._instances.get(instance_id)

    def all_instances(self) -> list[Instance]:
        with self._lock:
            return list(self._instances.values())

    def healthy_instances(self) -> list[Instance]:
        with self._lock:
            return [i for i in self._instances.values() if i.is_available()]

    def record_probe_success(self, instance_id: str) -> None:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state in (HealthState.DEREGISTERED,
                                              HealthState.DRAINING):
                return  # 已摘除/排水中的实例不因探测成功而复活
            inst.consecutive_failures = 0
            inst.state = HealthState.HEALTHY
            inst.reason = (UnavailableReason.WEIGHT_ZERO if inst.weight <= 0
                           else UnavailableReason.NONE)

    def record_probe_failure(self, instance_id: str) -> None:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state in (HealthState.DEREGISTERED,
                                              HealthState.DRAINING):
                return
            inst.consecutive_failures += 1
            if inst.consecutive_failures >= self._failure_threshold:
                inst.state = HealthState.UNHEALTHY
                inst.reason = UnavailableReason.PROBE_FAILED

    def acquire(self, instance_id: str) -> bool:
        """在途计数 +1（仅当实例可用且容量有余量）。与择路在同一锁内完成，避免竞态。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or not inst.is_available() or not inst.has_capacity():
                return False
            inst.inflight += 1
            return True

    def release(self, instance_id: str) -> None:
        """在途计数 -1，不允许出现负数；排水中在途清零时立即落地。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.inflight <= 0:
                return
            inst.inflight -= 1
            if inst.state == HealthState.DRAINING and inst.inflight == 0:
                self._finalize_drain(inst)

    def status(self) -> list[dict]:
        """返回每个实例的健康判定依据、权重、容量与排水进度。"""
        now = time.monotonic()
        with self._lock:
            return [
                {
                    "instance_id": i.instance_id,
                    "address": i.address,
                    "state": i.state.value,
                    "reason": i.effective_reason().value,
                    "consecutive_failures": i.consecutive_failures,
                    "inflight": i.inflight,
                    "failure_threshold": self._failure_threshold,
                    "weight": i.weight,
                    "max_inflight": i.max_inflight,
                    "at_capacity": not i.has_capacity(),
                    "drain_remaining": (
                        round(max(0.0, i.drain_deadline - now), 3)
                        if i.drain_deadline is not None else None
                    ),
                }
                for i in self._instances.values()
            ]
