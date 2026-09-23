"""择路策略：轮询、最少在途、按键一致性哈希。"""
from __future__ import annotations

import abc
import bisect
import hashlib
import threading

from .models import Instance, RouteDecision


def _hash(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest(), 16)


class Router(abc.ABC):
    """择路策略接口：从候选（健康）实例中选一个。"""

    name: str = "abstract"

    @abc.abstractmethod
    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        raise NotImplementedError

    def _none(self, key: str | None) -> RouteDecision:
        return RouteDecision(key=key, instance_id=None, strategy=self.name,
                             reason="no healthy candidate")


class RoundRobinRouter(Router):
    """轮询：游标单调递增，对候选数取模，保证均匀且可预期。"""

    name = "round_robin"

    def __init__(self) -> None:
        self._cursor = 0
        self._lock = threading.Lock()

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        if not candidates:
            return self._none(key)
        ordered = sorted(candidates, key=lambda i: i.instance_id)  # 稳定顺序
        with self._lock:
            idx = self._cursor % len(ordered)
            self._cursor += 1
        chosen = ordered[idx]
        return RouteDecision(key=key, instance_id=chosen.instance_id,
                             strategy=self.name, reason=f"cursor slot {idx}")


class LeastInflightRouter(Router):
    """最少在途：选 inflight 最小者，并列时按 id 字典序保证确定性。"""

    name = "least_inflight"

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        if not candidates:
            return self._none(key)
        chosen = min(candidates, key=lambda i: (i.inflight, i.instance_id))
        return RouteDecision(key=key, instance_id=chosen.instance_id,
                             strategy=self.name,
                             reason=f"inflight={chosen.inflight}")


class ConsistentHashRouter(Router):
    """按键一致性哈希：虚拟节点环，同键恒定落点，实例增减只影响少量键。"""

    name = "consistent_hash"

    def __init__(self, replicas: int = 100) -> None:
        self._replicas = replicas
        self._lock = threading.Lock()
        self._ring: list[tuple[int, str]] = []  # (hash, instance_id)，按 hash 有序
        self._ring_key: tuple[str, ...] = ()    # 构建环时的实例集合指纹

    def _build_ring(self, candidates: list[Instance]) -> None:
        ids = tuple(sorted(i.instance_id for i in candidates))
        with self._lock:
            if ids == self._ring_key:
                return
            ring: list[tuple[int, str]] = []
            for iid in ids:
                for r in range(self._replicas):
                    ring.append((_hash(f"{iid}#{r}"), iid))
            ring.sort()
            self._ring = ring
            self._ring_key = ids

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        if not candidates:
            return self._none(key)
        self._build_ring(candidates)
        with self._lock:
            ring = self._ring
        if key is None:
            # 无键时退化为按实例数轮到的第一个（调用方应提供键）
            chosen = ring[0][1]
            return RouteDecision(key=None, instance_id=chosen, strategy=self.name,
                                 reason="no key, ring head")
        h = _hash(key)
        idx = bisect.bisect_left(ring, (h,))
        if idx == len(ring):
            idx = 0
        chosen = ring[idx][1]
        return RouteDecision(key=key, instance_id=chosen, strategy=self.name,
                             reason=f"ring slot {idx}/{len(ring)}")
