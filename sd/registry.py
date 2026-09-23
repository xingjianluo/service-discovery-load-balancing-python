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
        raise NotImplementedError

    def deregister(self, instance_id: str) -> bool:
        raise NotImplementedError

    def get(self, instance_id: str) -> Instance | None:
        raise NotImplementedError

    def all_instances(self) -> list[Instance]:
        raise NotImplementedError

    def healthy_instances(self) -> list[Instance]:
        raise NotImplementedError

    def record_probe_success(self, instance_id: str) -> None:
        raise NotImplementedError

    def record_probe_failure(self, instance_id: str) -> None:
        raise NotImplementedError

    def acquire(self, instance_id: str) -> bool:
        """在途计数 +1（仅当实例可用）。"""
        raise NotImplementedError

    def release(self, instance_id: str) -> None:
        """在途计数 -1。"""
        raise NotImplementedError

    def status(self) -> list[dict]:
        """返回每个实例的健康判定依据与当前状态。"""
        raise NotImplementedError
