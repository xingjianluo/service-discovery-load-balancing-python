"""调度器：择路 + 在途计数 + 故障转移 + 熔断约束，线程安全。

并发语义（关键不变量）：
1. 「查候选 -> 查熔断 -> 占在途名额」在同一把锁内完成，保证某实例被选中
   后其 inflight 立即对其他线程可见，最少在途策略不会超发到同一实例，
   已摘除 / 熔断打开的实例永远不会被选中。
2. 真正的上游调用在锁外执行（不阻塞其他请求择路），但名额已占位。
3. 重试有次数上限，熔断器对失败计数，OPEN 后快速跳过，杜绝重试风暴。
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from .circuit_breaker import CircuitBreaker
from .load_balancer import LoadBalancer
from .registry import InstanceRegistry
from .transport import Transport
from .types import (
    AllInstancesUnavailable,
    BalancingStrategy,
    Instance,
    Request,
    Response,
    UnavailableReason,
    UpstreamError,
)

logger = logging.getLogger("scheduler.dispatcher")


@dataclass
class RetryConfig:
    max_retries: int = 2


@dataclass
class DispatchReport:
    """一次调度的完整轨迹，便于测试断言与日志解释。"""

    key: str
    success: bool = False
    result: Any = None
    selected: list[str] = field(default_factory=list)   # 每次尝试选中实例
    attempts: int = 0
    retries: int = 0
    reason: UnavailableReason | None = None


class Dispatcher:
    def __init__(
        self,
        registry: InstanceRegistry,
        load_balancer: LoadBalancer,
        circuit_breaker: CircuitBreaker,
        transport: Transport,
        retry: RetryConfig | None = None,
    ) -> None:
        self._registry = registry
        self._lb = load_balancer
        self._cb = circuit_breaker
        self._transport = transport
        self._retry = retry or RetryConfig()
        self._select_lock = threading.Lock()

    def dispatch(self, request: Request) -> Response:
        report = self.dispatch_report(request)
        if not report.success:
            raise AllInstancesUnavailable(
                f"dispatch failed key={request.key} reason={report.reason}"
            )
        return Response(
            instance_id=report.selected[-1],
            value=report.result,
            attempts=report.attempts,
            retries=report.retries,
        )

    def dispatch_report(self, request: Request) -> DispatchReport:
        report = DispatchReport(key=request.key)
        excluded: set[str] = set()  # 本次请求已试过的实例，故障转移时不再重复
        while True:
            selected = self._select(request, excluded)
            if selected is None:
                break
            excluded.add(selected.id)
            report.selected.append(selected.id)
            report.attempts += 1
            logger.info(
                "key=%s attempt=%d selected=%s strategy=%s",
                request.key,
                report.attempts,
                selected.id,
                self._lb.strategy.value,
            )
            try:
                value = self._transport.send(selected, request)
            except UpstreamError as exc:
                self._cb.record_failure(selected.id)
                logger.warning(
                    "key=%s instance=%s failed reason=%s retryable=%s",
                    request.key,
                    selected.id,
                    exc.reason.value,
                    exc.retryable,
                )
                self._release(selected.id)
                if (
                    exc.retryable
                    and report.retries < self._retry.max_retries
                ):
                    report.retries += 1
                    logger.info(
                        "key=%s retry=%d/%d failover",
                        request.key,
                        report.retries,
                        self._retry.max_retries,
                    )
                    continue
                report.reason = (
                    UnavailableReason.RETRIES_EXHAUSTED
                    if exc.retryable
                    else exc.reason
                )
                return report
            except Exception as exc:  # noqa: BLE001 - 非约定异常不重试
                self._release(selected.id)
                logger.warning(
                    "key=%s instance=%s non-retryable exception=%r",
                    request.key,
                    selected.id,
                    exc,
                )
                report.reason = UnavailableReason.RETRYABLE_UPSTREAM_ERROR
                return report
            else:
                self._cb.record_success(selected.id)
                self._release(selected.id)
                report.success = True
                report.result = value
                logger.info(
                    "key=%s success instance=%s attempts=%d retries=%d",
                    request.key,
                    selected.id,
                    report.attempts,
                    report.retries,
                )
                return report

        report.reason = (
            UnavailableReason.RETRIES_EXHAUSTED
            if report.attempts > 0
            else UnavailableReason.NO_CANDIDATE
        )
        logger.warning(
            "key=%s exhausted attempts=%d retries=%d reason=%s",
            request.key,
            report.attempts,
            report.retries,
            report.reason.value,
        )
        return report

    def _select(
        self, request: Request, excluded: set[str]
    ) -> Instance | None:
        # 原子地完成：取候选 -> 剔除熔断打开/已试过 -> 择路 -> 占在途名额。
        with self._select_lock:
            candidates = self._registry.candidates()
            usable = [
                c
                for c in candidates
                if c.id not in excluded and self._cb.allow(c.id)
            ]
            if not usable:
                return None
            inflight = {v.instance.id: v.inflight for v in self._registry.views()}
            chosen = self._lb.choose(usable, request.key, inflight)
            if chosen is None:
                return None
            if not self._registry.increment_inflight(chosen.id):
                # 理论不可达：候选与占位在同一临界区；保险起见直接返回无候选。
                return None
            return chosen

    def _release(self, instance_id: str) -> None:
        self._registry.decrement_inflight(instance_id)
