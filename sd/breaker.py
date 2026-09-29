"""熔断器：限制对故障实例的重试风暴。"""
from __future__ import annotations

import enum
import threading
import time


class BreakerState(enum.Enum):
    CLOSED = "closed"        # 正常放行
    OPEN = "open"            # 熔断，拒绝调用
    HALF_OPEN = "half_open"  # 冷却结束，放行一次试探


class CircuitBreaker:
    """每实例一个熔断器；打开时该实例不参与择路。

    规则：
    - CLOSED：连续失败达到阈值 -> OPEN。
    - OPEN：冷却期内拒绝；冷却结束后进入 HALF_OPEN。
    - HALF_OPEN：放行一次试探调用，成功 -> CLOSED，失败 -> 重新 OPEN。
    """

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 5.0) -> None:
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._failure_threshold = failure_threshold
        self._cooldown = cooldown_seconds

    def allow(self) -> bool:
        with self._lock:
            if self._state == BreakerState.CLOSED:
                return True
            if self._state == BreakerState.OPEN:
                if time.monotonic() - self._opened_at >= self._cooldown:
                    self._state = BreakerState.HALF_OPEN
                    return True  # 放行一次试探
                return False
            return False  # HALF_OPEN 且已有试探在途

    def would_allow(self) -> bool:
        """与 allow() 同样的放行判定，但不推进熔断器状态。

        供“无候选”原因归类使用：不能因为一次查询把 OPEN 提前翻成 HALF_OPEN、
        抢占唯一的试探名额。
        """
        with self._lock:
            if self._state == BreakerState.CLOSED:
                return True
            if self._state == BreakerState.OPEN:
                return time.monotonic() - self._opened_at >= self._cooldown
            return False

    def on_success(self) -> None:
        with self._lock:
            self._state = BreakerState.CLOSED
            self._failures = 0

    def on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == BreakerState.HALF_OPEN or \
                    self._failures >= self._failure_threshold:
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()

    @property
    def state(self) -> BreakerState:
        with self._lock:
            return self._state
