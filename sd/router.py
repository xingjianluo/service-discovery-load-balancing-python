"""择路策略：轮询、最少在途、按键一致性哈希。"""
from __future__ import annotations

import abc

from .models import Instance, RouteDecision


class Router(abc.ABC):
    """择路策略接口：从候选实例中选一个。"""

    name: str = "abstract"

    @abc.abstractmethod
    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        raise NotImplementedError


class RoundRobinRouter(Router):
    name = "round_robin"

    def __init__(self) -> None:
        self._cursor = 0

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        raise NotImplementedError


class LeastInflightRouter(Router):
    name = "least_inflight"

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        raise NotImplementedError


class ConsistentHashRouter(Router):
    name = "consistent_hash"

    def __init__(self, replicas: int = 100) -> None:
        self._replicas = replicas

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        raise NotImplementedError
