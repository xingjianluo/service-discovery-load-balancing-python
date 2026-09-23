"""本地上游实例模拟器：用线程内行为模拟延迟、失败与超时。"""
from __future__ import annotations

import threading

from .models import FailureKind


class SimulatedUpstream:
    """模拟一个上游实例的调用行为，可编程控制失败/延迟。"""

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self._lock = threading.Lock()
        self._fail_next = 0          # 接下来 N 次调用返回可重试失败
        self._latency = 0.0          # 每次调用的模拟延迟（秒）
        self._hang = False           # 是否挂起（触发超时）
        self.calls = 0

    def set_fail_next(self, n: int) -> None:
        raise NotImplementedError

    def set_latency(self, seconds: float) -> None:
        raise NotImplementedError

    def set_hang(self, hang: bool) -> None:
        raise NotImplementedError

    def call(self, timeout: float) -> tuple[bool, FailureKind | None, str]:
        """模拟一次调用，返回 (ok, failure_kind, detail)。"""
        raise NotImplementedError
