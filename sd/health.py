"""健康检查器：后台线程周期探测实例，驱动健康状态收敛。"""
from __future__ import annotations

import logging
import threading

from .registry import ServiceRegistry
from .simulator import SimulatedUpstream

logger = logging.getLogger("sd.health")


class HealthChecker:
    """周期性对每个已注册实例发起探测调用。

    - 探测成功 -> registry.record_probe_success（可恢复健康）
    - 探测失败 -> registry.record_probe_failure（累计到阈值后摘除）
    - 已显式摘除（DEREGISTERED）的实例不会被复活。
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        upstreams: dict[str, SimulatedUpstream],
        interval: float = 0.5,
        probe_timeout: float = 0.2,
    ) -> None:
        self._registry = registry
        self._upstreams = upstreams
        self._interval = interval
        self._probe_timeout = probe_timeout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def probe_once(self) -> None:
        """执行一轮探测（同步，便于测试直接调用）。"""
        for inst in self._registry.all_instances():
            upstream = self._upstreams.get(inst.instance_id)
            if upstream is None:
                continue
            ok, _, detail = upstream.call(self._probe_timeout)
            if ok:
                self._registry.record_probe_success(inst.instance_id)
            else:
                self._registry.record_probe_failure(inst.instance_id)
            logger.info("probe instance=%s ok=%s detail=%s",
                        inst.instance_id, ok, detail)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            self.probe_once()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
