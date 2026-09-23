"""服务注册表：注册、摘除、健康状态收敛（线程安全）。"""
from __future__ import annotations

import threading

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
            instance.reason = UnavailableReason.NONE
            instance.consecutive_failures = 0
            self._instances[instance.instance_id] = instance

    def deregister(self, instance_id: str) -> bool:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None:
                return False
            inst.state = HealthState.DEREGISTERED
            inst.reason = UnavailableReason.MANUALLY_REMOVED
            return True

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
            if inst is None or inst.state == HealthState.DEREGISTERED:
                return  # 已摘除的实例不因探测成功而复活
            inst.consecutive_failures = 0
            inst.state = HealthState.HEALTHY
            inst.reason = UnavailableReason.NONE

    def record_probe_failure(self, instance_id: str) -> None:
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or inst.state == HealthState.DEREGISTERED:
                return
            inst.consecutive_failures += 1
            if inst.consecutive_failures >= self._failure_threshold:
                inst.state = HealthState.UNHEALTHY
                inst.reason = UnavailableReason.PROBE_FAILED

    def acquire(self, instance_id: str) -> bool:
        """在途计数 +1（仅当实例可用）。与择路在同一锁内完成，避免竞态。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is None or not inst.is_available():
                return False
            inst.inflight += 1
            return True

    def release(self, instance_id: str) -> None:
        """在途计数 -1，不允许出现负数。"""
        with self._lock:
            inst = self._instances.get(instance_id)
            if inst is not None and inst.inflight > 0:
                inst.inflight -= 1

    def status(self) -> list[dict]:
        """返回每个实例的健康判定依据与当前状态。"""
        with self._lock:
            return [
                {
                    "instance_id": i.instance_id,
                    "address": i.address,
                    "state": i.state.value,
                    "reason": i.reason.value,
                    "consecutive_failures": i.consecutive_failures,
                    "inflight": i.inflight,
                    "failure_threshold": self._failure_threshold,
                }
                for i in self._instances.values()
            ]
