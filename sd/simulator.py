"""本地上游实例模拟器：用线程内行为模拟延迟、失败与超时。"""
from __future__ import annotations

import threading
import time

from .models import FailureKind


class SimulatedUpstream:
    """模拟一个上游实例的调用行为，可编程控制失败/延迟/挂起。

    - set_fail_next(n)：接下来 n 次调用返回可重试失败（模拟 502/503）。
    - set_latency(s)：每次调用耗时 s 秒。
    - set_hang(True)：调用永不返回（配合调用方超时触发 TIMEOUT）。
    - set_gate(event)：调用进入后先在闸门上等待（用于测试在途请求），
      闸门放行后按正常调用返回成功。
    """

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self._lock = threading.Lock()
        self._fail_next = 0
        self._latency = 0.0
        self._hang = False
        self._gate: threading.Event | None = None
        self.calls = 0

    def set_fail_next(self, n: int) -> None:
        with self._lock:
            self._fail_next = n

    def set_latency(self, seconds: float) -> None:
        with self._lock:
            self._latency = seconds

    def set_hang(self, hang: bool) -> None:
        with self._lock:
            self._hang = hang

    def set_gate(self, event: threading.Event | None) -> None:
        with self._lock:
            self._gate = event

    def call(self, timeout: float) -> tuple[bool, FailureKind | None, str]:
        """模拟一次调用，返回 (ok, failure_kind, detail)。超时由本方法内执行。"""
        with self._lock:
            self.calls += 1
            fail = self._fail_next > 0
            if fail:
                self._fail_next -= 1
            latency = self._latency
            hang = self._hang
            gate = self._gate
        if gate is not None:
            gate.wait()  # 测试用：在途请求挂在这里，直到测试放行
        if hang:
            time.sleep(timeout + 0.5)  # 模拟挂起，必然超时
            return False, FailureKind.TIMEOUT, "upstream hung"
        if latency > 0:
            time.sleep(latency)
            if latency > timeout:
                return False, FailureKind.TIMEOUT, f"latency {latency}s > timeout {timeout}s"
        if fail:
            return False, FailureKind.RETRYABLE, "simulated 503"
        return True, None, "ok"
