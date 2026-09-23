"""实例注册中心：注册、摘除、健康状态收敛与查询。

线程安全：所有可变状态由单一 RLock 保护。
状态机：
    register()        -> HEALTHY（新实例默认可用，随后由探测持续校验）
    mark_unhealthy()  -> UNHEALTHY（携带可区分原因）
    mark_healthy()    -> HEALTHY（探测恢复后重新参与调度）
    deregister()      -> DEREGISTERED（永久摘除，保留条目便于查询）
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable

from .types import (
    CircuitState,
    Instance,
    InstanceState,
    InstanceView,
    UnavailableReason,
)


@dataclass
class _Entry:
    instance: Instance
    state: InstanceState = InstanceState.HEALTHY
    state_reason: UnavailableReason | None = None
    consecutive_failures: int = 0
    last_probe_at: float | None = None
    inflight: int = 0


# 熔断器状态提供者（由装配层注入，避免循环依赖）。
CircuitProvider = Callable[[str], tuple[CircuitState, float | None]]


def _default_circuit(_id: str) -> tuple[CircuitState, float | None]:
    return CircuitState.CLOSED, None


class InstanceRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, _Entry] = {}
        self._circuit_provider: CircuitProvider = _default_circuit

    def set_circuit_provider(self, provider: CircuitProvider) -> None:
        with self._lock:
            self._circuit_provider = provider

    def register(self, instance: Instance) -> None:
        """注册（或重新注册）实例。重新注册会从摘除态恢复。"""
        with self._lock:
            existing = self._entries.get(instance.id)
            if existing is None:
                self._entries[instance.id] = _Entry(instance=instance)
            else:
                existing.instance = instance
                existing.state = InstanceState.HEALTHY
                existing.state_reason = None
                existing.consecutive_failures = 0
                existing.last_probe_at = None
                existing.inflight = 0

    def deregister(self, instance_id: str) -> None:
        """人工摘除：新请求不得再路由到该实例。"""
        with self._lock:
            entry = self._entries.get(instance_id)
            if entry is None:
                raise KeyError(instance_id)
            entry.state = InstanceState.DEREGISTERED
            entry.state_reason = UnavailableReason.DEREGISTERED

    def mark_healthy(self, instance_id: str) -> None:
        with self._lock:
            entry = self._require(instance_id)
            entry.consecutive_failures = 0
            if entry.state == InstanceState.DEREGISTERED:
                # 已人工摘除的实例不能被探测自动恢复，必须重新注册。
                return
            entry.state = InstanceState.HEALTHY
            entry.state_reason = None

    def mark_unhealthy(self, instance_id: str, reason: UnavailableReason) -> None:
        with self._lock:
            entry = self._require(instance_id)
            if entry.state == InstanceState.DEREGISTERED:
                return
            entry.consecutive_failures += 1
            entry.state = InstanceState.UNHEALTHY
            entry.state_reason = reason

    def note_probe(self, instance_id: str, ok: bool, at: float) -> None:
        with self._lock:
            entry = self._require(instance_id)
            entry.last_probe_at = at
            if ok:
                self.mark_healthy(instance_id)
            else:
                self.mark_unhealthy(instance_id, UnavailableReason.PROBE_FAILED)

    def touch_probe(self, instance_id: str, at: float) -> None:
        """仅记录探测时间，不改变健康状态（恢复观察期使用）。"""
        with self._lock:
            self._require(instance_id).last_probe_at = at

    def increment_inflight(self, instance_id: str) -> bool:
        """在途计数 +1。仅健康实例允许，返回是否成功占位。"""
        with self._lock:
            entry = self._entries.get(instance_id)
            if entry is None or entry.state != InstanceState.HEALTHY:
                return False
            entry.inflight += 1
            return True

    def decrement_inflight(self, instance_id: str) -> None:
        with self._lock:
            entry = self._entries.get(instance_id)
            if entry is not None and entry.inflight > 0:
                entry.inflight -= 1

    def get(self, instance_id: str) -> Instance:
        with self._lock:
            return self._require(instance_id).instance

    def is_healthy(self, instance_id: str) -> bool:
        with self._lock:
            entry = self._entries.get(instance_id)
            return entry is not None and entry.state == InstanceState.HEALTHY

    def candidates(self) -> list[Instance]:
        """当前可参与调度的实例（已注册且健康），按注册顺序。"""
        with self._lock:
            return [
                e.instance
                for e in self._entries.values()
                if e.state == InstanceState.HEALTHY
            ]

    def view(self, instance_id: str) -> InstanceView:
        with self._lock:
            entry = self._require(instance_id)
            circuit, until = self._circuit_provider(instance_id)
            return self._to_view(entry, circuit, until)

    def views(self) -> list[InstanceView]:
        with self._lock:
            result = []
            for entry in self._entries.values():
                circuit, until = self._circuit_provider(entry.instance.id)
                result.append(self._to_view(entry, circuit, until))
            return result

    def _require(self, instance_id: str) -> _Entry:
        entry = self._entries.get(instance_id)
        if entry is None:
            raise KeyError(instance_id)
        return entry

    @staticmethod
    def _to_view(
        entry: _Entry, circuit: CircuitState, circuit_until: float | None
    ) -> InstanceView:
        return InstanceView(
            instance=entry.instance,
            state=entry.state,
            state_reason=entry.state_reason.value
            if entry.state_reason is not None
            else None,
            consecutive_failures=entry.consecutive_failures,
            last_probe_at=entry.last_probe_at,
            inflight=entry.inflight,
            circuit=circuit,
            circuit_until=circuit_until,
        )
