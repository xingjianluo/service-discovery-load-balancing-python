"""核心数据模型：实例、健康状态、不可用原因、路由结果。"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field


class HealthState(enum.Enum):
    """实例健康状态。"""

    HEALTHY = "healthy"        # 正常参与调度
    UNHEALTHY = "unhealthy"    # 探测失败，暂时摘除
    DRAINING = "draining"      # 优雅下线中：不接新请求，在途请求继续跑完
    DEREGISTERED = "deregistered"  # 被显式摘除（含排水完成后落地）


class UnavailableReason(enum.Enum):
    """实例不可用的可区分原因。"""

    NONE = "none"                        # 实例可用
    PROBE_FAILED = "probe_failed"        # 连续健康探测失败
    MANUALLY_REMOVED = "manually_removed"  # 被显式摘除
    CIRCUIT_OPEN = "circuit_open"        # 熔断器打开
    WEIGHT_ZERO = "weight_zero"          # 权重为 0，不参与分配新流量
    DRAINING = "draining"                # 排水中：不接新请求，在途继续
    DRAINED = "drained"                  # 排水完成（在途清零或超时）后落地
    CAPACITY_FULL = "capacity_full"      # 并发容量达到上限，暂时不可调度


class FailureKind(enum.Enum):
    """一次调用的失败归类。"""

    RETRYABLE = "retryable"      # 可重试（如 502/503、连接错误）
    TIMEOUT = "timeout"          # 超时，可重试
    NON_RETRYABLE = "non_retryable"  # 不可重试（如 400）


@dataclass
class Instance:
    """上游实例：用本地模拟进程/端口表示。"""

    instance_id: str
    host: str
    port: int
    state: HealthState = HealthState.HEALTHY
    reason: UnavailableReason = UnavailableReason.NONE
    consecutive_failures: int = 0
    inflight: int = 0  # 在途请求数（并发安全由 registry 的锁保证）
    weight: int = 1  # 流量分配权重；0 表示不参与调度（不影响在途）
    max_inflight: int = 0  # 并发容量上限；0 表示不限制
    drain_deadline: float | None = None  # 排水截止时刻（monotonic），非空表示排水中
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def is_available(self) -> bool:
        """是否可接新请求：健康且权重大于 0（排水/摘除/权重 0 均不可用）。"""
        return self.state == HealthState.HEALTHY and self.weight > 0

    def has_capacity(self) -> bool:
        """并发容量是否还有余量（max_inflight<=0 表示不限）。"""
        return self.max_inflight <= 0 or self.inflight < self.max_inflight


@dataclass
class RouteDecision:
    """一次择路的结果记录，用于日志与测试断言。"""

    key: str | None
    instance_id: str | None
    strategy: str
    reason: str  # 为什么选中/未选中


@dataclass
class CallResult:
    """一次（可能含重试的）调用的最终结果。"""

    ok: bool
    instance_id: str | None
    attempts: int
    failure_kind: FailureKind | None
    detail: str
