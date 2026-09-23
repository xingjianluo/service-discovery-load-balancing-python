"""健康检查：后台主动探测，连续失败收敛为不健康，恢复后重新参与调度。

规则（见 HealthCheckConfig）：
- 每 interval 秒对所有「未摘除」实例探测一次；
- 连续 failure_threshold 次失败 -> UNHEALTHY(probe_failed)，调度器随即跳过；
- UNHEALTHY 实例继续被探测，连续 success_threshold 次成功 -> HEALTHY，
  自动重新参与调度；
- 已人工摘除（DEREGISTERED）的实例不参与探测，也不会被自动恢复。
"""
from __future__ import annotations

import http.client
import logging
import threading
import time
from collections.abc import Callable

from .registry import InstanceRegistry
from .types import Instance, UnavailableReason

logger = logging.getLogger("scheduler.health")

Probe = Callable[[Instance], bool]


def http_probe(path: str = "/health", timeout: float = 0.3) -> Probe:
    """构造一个真实 HTTP 探测函数：2xx 视为健康。"""

    def _probe(instance: Instance) -> bool:
        try:
            conn = http.client.HTTPConnection(
                instance.address, instance.port, timeout=timeout
            )
            try:
                conn.request("GET", path)
                resp = conn.getresponse()
                ok = 200 <= resp.status < 300
                resp.read()
                return ok
            finally:
                conn.close()
        except OSError:
            return False

    return _probe


class HealthCheckConfig:
    def __init__(
        self,
        interval: float = 0.2,
        failure_threshold: int = 3,
        success_threshold: int = 1,
    ) -> None:
        self.interval = interval
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold


class HealthChecker:
    def __init__(
        self,
        registry: InstanceRegistry,
        probe: Probe,
        config: HealthCheckConfig | None = None,
    ) -> None:
        self._registry = registry
        self._probe = probe
        self._config = config or HealthCheckConfig()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # 每个实例独立的连续成功计数，满足 success_threshold 才上报恢复。
        self._success_streak: dict[str, int] = {}

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="health-checker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def check_once(self) -> None:
        now = time.monotonic()
        views = self._registry.views()
        for view in views:
            inst = view.instance
            if view.state.value == "deregistered":
                continue
            ok = False
            try:
                ok = bool(self._probe(inst))
            except Exception:  # noqa: BLE001 - 探测异常一律视为失败
                ok = False

            with self._lock:
                if ok:
                    self._success_streak[inst.id] = (
                        self._success_streak.get(inst.id, 0) + 1
                    )
                    if (
                        view.state.value == "unhealthy"
                        and self._success_streak[inst.id]
                        < self._config.success_threshold
                    ):
                        # 恢复期内继续观察：不改变健康状态，仅刷新探测时间。
                        self._registry.touch_probe(inst.id, now)
                        logger.debug(
                            "instance=%s recovering streak=%d",
                            inst.id,
                            self._success_streak[inst.id],
                        )
                        continue
                    self._success_streak[inst.id] = 0
                    self._registry.note_probe(inst.id, True, now)
                    if view.state.value != "healthy":
                        logger.info("instance=%s state -> healthy", inst.id)
                else:
                    self._success_streak[inst.id] = 0
                    self._registry.note_probe(
                        inst.id, False, now
                    )
                    if (
                        view.consecutive_failures + 1
                        >= self._config.failure_threshold
                    ):
                        logger.warning(
                            "instance=%s state -> unhealthy reason=%s",
                            inst.id,
                            UnavailableReason.PROBE_FAILED.value,
                        )

    def _run(self) -> None:
        while not self._stop.wait(self._config.interval):
            try:
                self.check_once()
            except Exception:  # noqa: BLE001 - 后台线程不能因单次异常退出
                logger.exception("health check round failed")
