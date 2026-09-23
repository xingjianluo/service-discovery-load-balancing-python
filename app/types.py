"""公共类型定义：实例、请求/响应、异常与状态枚举。"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any


class BalancingStrategy(str, enum.Enum):
    """择路策略。"""

    ROUND_ROBIN = "round_robin"
    LEAST_INFLIGHT = "least_inflight"
    CONSISTENT_HASH = "consistent_hash"


class InstanceState(str, enum.Enum):
    """实例健康状态（注册表视角）。"""

    UNKNOWN = "unknown"          # 刚注册，尚未确认
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"      # 主动探测连续失败/不可达
    DEREGISTERED = "deregistered"  # 已人工摘除


class CircuitState(str, enum.Enum):
    """熔断器状态。"""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class UnavailableReason(str, enum.Enum):
    """实例/请求不可用的可区分原因。"""

    DEREGISTERED = "deregistered"
    PROBE_FAILED = "probe_failed"
    CIRCUIT_OPEN = "circuit_open"
    RETRYABLE_UPSTREAM_ERROR = "retryable_upstream_error"
    TIMEOUT = "timeout"
    NO_CANDIDATE = "no_candidate"
    RETRIES_EXHAUSTED = "retries_exhausted"


@dataclass(frozen=True)
class Instance:
    """一个上游实例的静态描述。"""

    id: str
    address: str
    port: int
    weight: int = 1
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def endpoint(self) -> str:
        return f"{self.address}:{self.port}"


@dataclass
class Request:
    """一次待转发的请求。"""

    key: str
    payload: Any = None
    timeout: float = 1.0


@dataclass
class Response:
    """上游返回结果。"""

    instance_id: str
    value: Any
    attempts: int
    retries: int


@dataclass
class InstanceView:
    """实例当前完整状态的查询视图。"""

    instance: Instance
    state: InstanceState
    state_reason: str | None
    consecutive_failures: int
    last_probe_at: float | None
    inflight: int
    circuit: CircuitState
    circuit_until: float | None


class UpstreamError(Exception):
    """上游错误基类。"""

    retryable: bool = False
    reason: UnavailableReason = UnavailableReason.RETRYABLE_UPSTREAM_ERROR


class RetryableUpstreamError(UpstreamError):
    retryable = True
    reason = UnavailableReason.RETRYABLE_UPSTREAM_ERROR


class FatalUpstreamError(UpstreamError):
    retryable = False
    reason = UnavailableReason.RETRYABLE_UPSTREAM_ERROR


class UpstreamTimeout(UpstreamError):
    retryable = True
    reason = UnavailableReason.TIMEOUT


class AllInstancesUnavailable(UpstreamError):
    """所有候选实例都不可用时抛出。"""

    retryable = False
    reason = UnavailableReason.NO_CANDIDATE


@dataclass
class DispatchError:
    """调度失败的结构化结果。"""

    reason: UnavailableReason
    message: str
    attempts: int
    tried_instances: list[str]
