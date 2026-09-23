"""熔断器：按实例统计连续失败。

状态机：
    CLOSED --连续失败达阈值--> OPEN
    OPEN --open_duration 后有探测请求--> HALF_OPEN（限流放行）
    HALF_OPEN --成功--> CLOSED / --失败--> OPEN（重新计时）

OPEN 期间调度器直接跳过该实例（快速失败语义），避免重试风暴。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .types import CircuitState


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    open_duration: float = 2.0
    half_open_max_calls: int = 1


@dataclass
class _BreakerEntry:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    open_until: float | None = None
    half_open_inflight: int = 0


def _now() -> float:
    return time.monotonic()


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._lock = threading.Lock()
        self._entries: dict[str, _BreakerEntry] = {}

    def allow(self, instance_id: str) -> bool:
        with self._lock:
            entry = self._entry(instance_id)
            now = _now()
            if entry.state is CircuitState.CLOSED:
                return True
            if entry.state is CircuitState.OPEN:
                if entry.open_until is not None and now >= entry.open_until:
                    entry.state = CircuitState.HALF_OPEN
                    entry.half_open_inflight = 1
                    return True
                return False
            # HALF_OPEN：仅放行有限数量的探测调用，其余快速失败。
            if entry.half_open_inflight < self._config.half_open_max_calls:
                entry.half_open_inflight += 1
                return True
            return False

    def record_success(self, instance_id: str) -> None:
        with self._lock:
            entry = self._entry(instance_id)
            entry.state = CircuitState.CLOSED
            entry.failures = 0
            entry.open_until = None
            entry.half_open_inflight = 0

    def record_failure(self, instance_id: str) -> None:
        with self._lock:
            entry = self._entry(instance_id)
            entry.failures += 1
            if entry.state is CircuitState.HALF_OPEN:
                self._trip(entry)
            elif entry.failures >= self._config.failure_threshold:
                self._trip(entry)

    def state(self, instance_id: str) -> CircuitState:
        with self._lock:
            entry = self._entry(instance_id)
            self._maybe_half_open(entry)
            return entry.state

    def open_until(self, instance_id: str) -> float | None:
        with self._lock:
            entry = self._entry(instance_id)
            self._maybe_half_open(entry)
            return entry.open_until if entry.state is CircuitState.OPEN else None

    def _trip(self, entry: _BreakerEntry) -> None:
        entry.state = CircuitState.OPEN
        entry.open_until = _now() + self._config.open_duration
        entry.half_open_inflight = 0

    def _maybe_half_open(self, entry: _BreakerEntry) -> None:
        if (
            entry.state is CircuitState.OPEN
            and entry.open_until is not None
            and _now() >= entry.open_until
        ):
            entry.state = CircuitState.HALF_OPEN
            entry.half_open_inflight = 0

    def _entry(self, instance_id: str) -> _BreakerEntry:
        entry = self._entries.get(instance_id)
        if entry is None:
            entry = _BreakerEntry()
            self._entries[instance_id] = entry
        return entry
