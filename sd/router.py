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
    """平滑加权轮询（nginx 风格）：每轮给各实例当前值加上权重，选最大者并减去总权重。

    权重相同时退化为均匀轮询；权重变化立即生效，流量按比例平滑过渡。
    """

    name = "round_robin"

    def __init__(self) -> None:
        self._current: dict[str, int] = {}  # instance_id -> 平滑轮询当前值
        self._lock = threading.Lock()

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        if not candidates:
            return self._none(key)
        ordered = sorted(candidates, key=lambda i: i.instance_id)  # 稳定顺序
        with self._lock:
            live = {i.instance_id for i in ordered}
            for stale in set(self._current) - live:
                del self._current[stale]  # 清理已下线实例的游标状态
            total = sum(i.weight for i in ordered)
            best = ordered[0]
            for inst in ordered:
                cw = self._current.get(inst.instance_id, 0) + inst.weight
                self._current[inst.instance_id] = cw
                if cw > self._current[best.instance_id]:
                    best = inst
            self._current[best.instance_id] -= total
        return RouteDecision(key=key, instance_id=best.instance_id,
                             strategy=self.name,
                             reason=f"smooth wrr weight={best.weight}")


class LeastInflightRouter(Router):
    """加权最少在途：选 inflight/weight 最小者，并列时按 id 字典序保证确定性。"""

    name = "least_inflight"

    def pick(self, candidates: list[Instance], key: str | None = None) -> RouteDecision:
        if not candidates:
            return self._none(key)
        chosen = min(candidates, key=lambda i: (i.inflight / i.weight, i.instance_id))
        return RouteDecision(key=key, instance_id=chosen.instance_id,
                             strategy=self.name,
                             reason=f"inflight={chosen.inflight} weight={chosen.weight}")


class ConsistentHashRouter(Router):
    """按键加权一致性哈希：虚拟节点数与权重成正比，同键恒定落点，变更只影响少量键。"""

    name = "consistent_hash"

    def __init__(self, replicas: int = 100) -> None:
        self._replicas = replicas
        self._lock = threading.Lock()
        self._ring: list[tuple[int, str]] = []  # (hash, instance_id)，按 hash 有序
        self._ring_key: tuple[tuple[str, int], ...] = ()  # (实例, 权重) 集合指纹

    def _build_ring(self, candidates: list[Instance]) -> None:
        fingerprint = tuple(sorted((i.instance_id, i.weight) for i in candidates))
        with self._lock:
            if fingerprint == self._ring_key:
                return
            ring: list[tuple[int, str]] = []
            for iid, weight in fingerprint:
                for r in range(self._replicas * weight):
                    ring.append((_hash(f"{iid}#{r}"), iid))
            ring.sort()
            self._ring = ring
            self._ring_key = fingerprint

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
