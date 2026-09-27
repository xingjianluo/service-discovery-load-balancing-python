"""核心数据模型：实例、健康状态、不可用原因、路由结果。"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field


class HealthState(enum.Enum):
    """实例健康状态。"""

    HEALTHY = "healthy"        # 正常参与调度
    UNHEALTHY = "unhealthy"    # 探测失败，暂时摘除
    DEREGISTERED = "deregistered"  # 被显式摘除
    DRAINING = "draining"      # 优雅下线排水中：不接新请求，在途请求跑完
    DRAINED = "drained"        # 排水结束，已落地为不可用


class UnavailableReason(enum.Enum):
    """实例不可用的可区分原因。"""

    NONE = "none"                        # 实例可用
    PROBE_FAILED = "probe_failed"        # 连续健康探测失败
    MANUALLY_REMOVED = "manually_removed"  # 被显式摘除
    CIRCUIT_OPEN = "circuit_open"        # 熔断器打开
    WEIGHT_ZERO = "weight_zero"          # 权重被调成 0，不接新请求
    DRAINING = "draining"                # 正在排水，不接新请求
    DRAINED = "drained"                  # 排水时限到或在途清零，已落地
    AT_CAPACITY = "at_capacity"          # 并发请求数已达实例容量上限


class FailureKind(enum.Enum):
    """一次调用的失败归类。"""

    RETRYABLE = "retryable"      # 可重试（如 502/503、连接错误）
    TIMEOUT = "timeout"          # 超时，可重试
    NON_RETRYABLE = "non_retryable"  # 不可重试（如 400）
    CAPACITY_EXHAUSTED = "capacity_exhausted"  # 所有候选都到并发上限，快速失败


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
    weight: int = 1  # 流量权重；1 为默认均匀，0 表示不接新请求
    max_concurrency: int = 0  # 并发容量上限；0 表示不限
    drain_deadline: float | None = None  # 排水时限（monotonic），None 表示未排水
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def is_available(self) -> bool:
        return self.state == HealthState.HEALTHY

    def is_draining(self) -> bool:
        return self.state == HealthState.DRAINING

    def at_capacity(self) -> bool:
        """是否已达并发容量上限（0 表示不限）。调用方需持注册表锁读取。"""
        return self.max_concurrency > 0 and self.inflight >= self.max_concurrency

    def scheduling_reason(self) -> UnavailableReason:
        """当前不参与调度的最具体原因；可调度时为 NONE。调用方需持注册表锁。"""
        if self.state == HealthState.DRAINING:
            return UnavailableReason.DRAINING
        if self.state == HealthState.DRAINED:
            return UnavailableReason.DRAINED
        if self.state == HealthState.UNHEALTHY:
            return self.reason  # probe_failed
        if self.state == HealthState.DEREGISTERED:
            return self.reason  # manually_removed
        if self.state == HealthState.HEALTHY:
            if self.weight == 0:
                return UnavailableReason.WEIGHT_ZERO
            if self.at_capacity():
                return UnavailableReason.AT_CAPACITY
        return UnavailableReason.NONE


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
