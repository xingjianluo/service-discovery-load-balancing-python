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


class UnavailableReason(enum.Enum):
    """实例不可用的可区分原因。"""

    NONE = "none"                        # 实例可用
    PROBE_FAILED = "probe_failed"        # 连续健康探测失败
    MANUALLY_REMOVED = "manually_removed"  # 被显式摘除
    CIRCUIT_OPEN = "circuit_open"        # 熔断器打开


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
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def is_available(self) -> bool:
        return self.state == HealthState.HEALTHY


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
