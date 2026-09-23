"""负载均衡器：组合注册表、择路策略、熔断与有限重试。"""
from __future__ import annotations

import threading

from .breaker import CircuitBreaker
from .models import CallResult, RouteDecision
from .registry import ServiceRegistry
from .router import Router
from .simulator import SimulatedUpstream


class LoadBalancer:
    """对外入口：select + 调用 + 故障转移（有界重试）。"""

    def __init__(
        self,
        registry: ServiceRegistry,
        router: Router,
        upstreams: dict[str, SimulatedUpstream],
        max_retries: int = 2,
        call_timeout: float = 1.0,
    ) -> None:
        self._registry = registry
        self._router = router
        self._upstreams = upstreams
        self._max_retries = max_retries
        self._timeout = call_timeout
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
        self.decision_log: list[RouteDecision] = []

    def call(self, key: str | None = None) -> CallResult:
        raise NotImplementedError

    def breaker_state(self, instance_id: str) -> str:
        raise NotImplementedError
