"""熔断器：限制对故障实例的重试风暴。"""
from __future__ import annotations

import enum
import threading


class BreakerState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """每实例一个熔断器；打开时该实例不参与择路。"""

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 5.0) -> None:
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._failure_threshold = failure_threshold
        self._cooldown = cooldown_seconds

    def allow(self) -> bool:
        raise NotImplementedError

    def on_success(self) -> None:
        raise NotImplementedError

    def on_failure(self) -> None:
        raise NotImplementedError

    @property
    def state(self) -> BreakerState:
        raise NotImplementedError
