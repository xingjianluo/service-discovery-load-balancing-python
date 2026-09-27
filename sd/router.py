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
    """择路策略接口：从候选（健康、有容量）实例中选一个。"""

    name: str = "abstract"

    @abc.abstractmethod
    def pick(
        self,
        candidates: list[Instance],
        key: str | None = None,
        exclude: frozenset[str] | None = None,
    ) -> RouteDecision:
        raise NotImplementedError

    def _none(
        self,
        key: str | None,
        reason: str = "no healthy candidate",
    ) -> RouteDecision:
        return RouteDecision(key=key, instance_id=None, strategy=self.name,
                             reason=reason)


class RoundRobinRouter(Router):
    """平滑加权轮询（Nginx SWRR）：按权重成比例且交错分布。

    每轮 current += weight，选 current 最大者，再让被选者 current -= 权重总和。
    权重热调整下一轮立即生效；权重相等时退化回按 id 字典序的均匀轮转，
    与改造前行为保持一致。并列时取 id 最小者，保证确定性。
    """

    name = "round_robin"

    def __init__(self) -> None:
        self._current: dict[str, int] = {}
        self._lock = threading.Lock()

    def pick(
        self,
        candidates: list[Instance],
        key: str | None = None,
        exclude: frozenset[str] | None = None,
    ) -> RouteDecision:
        exclude = exclude or frozenset()
        pool = [i for i in candidates if i.instance_id not in exclude]
        if not pool:
            return self._none(key, "no candidate after excluding tried instances")
        ordered = sorted(pool, key=lambda i: i.instance_id)  # 稳定顺序
        with self._lock:
            # 清理已不在候选集合的历史状态，保证热调整平滑且内存有界
            live = {i.instance_id for i in ordered}
            for iid in list(self._current):
                if iid not in live:
                    del self._current[iid]
            total = 0
            for inst in ordered:
                cw = self._current.get(inst.instance_id, 0) + max(0, inst.weight)
                self._current[inst.instance_id] = cw
                total += max(0, inst.weight)
            # current 最大者；并列时取 id 最小者，保证确定性
            chosen = min(
                ordered,
                key=lambda i: (-self._current[i.instance_id], i.instance_id),
            )
            self._current[chosen.instance_id] -= total
            cw_after = self._current[chosen.instance_id]
        return RouteDecision(key=key, instance_id=chosen.instance_id,
                             strategy=self.name,
                             reason=f"weight={chosen.weight} cw={cw_after}")


class LeastInflightRouter(Router):
    """加权最少在途：选“在途/权重”占用率最低的实例，把余量留给高权重者。

    权重相等时与改造前完全一致（最少在途，并列按 id 字典序）。
    """

    name = "least_inflight"

    def pick(
        self,
        candidates: list[Instance],
        key: str | None = None,
        exclude: frozenset[str] | None = None,
    ) -> RouteDecision:
        exclude = exclude or frozenset()
        pool = [i for i in candidates if i.instance_id not in exclude]
        if not pool:
            return self._none(key, "no candidate after excluding tried instances")

        def load(inst: Instance) -> float:
            return inst.inflight / max(1, inst.weight)

        chosen = min(pool, key=lambda i: (load(i), i.instance_id))
        return RouteDecision(key=key, instance_id=chosen.instance_id,
                             strategy=self.name,
                             reason=f"inflight={chosen.inflight} weight={chosen.weight}")


class ConsistentHashRouter(Router):
    """按键一致性哈希：虚拟节点环，同键恒定落点，实例增减只影响少量键。

    每实例虚拟节点数 = 基础副本数 × 权重（默认权重 1 时与改造前一致），
    权重热调整后环按 (实例, 权重) 指纹重建；权重 0 的实例不进环。
    故障转移时沿环跳过已试过的实例，而不是反复落在同一个点。
    """

    name = "consistent_hash"

    def __init__(self, replicas: int = 100) -> None:
        self._replicas = replicas
        self._lock = threading.Lock()
        self._ring: list[tuple[int, str]] = []  # (hash, instance_id)，按 hash 有序
        self._ring_key: tuple[tuple[str, int], ...] = ()  # 建环指纹 (iid, weight)

    def _build_ring(self, candidates: list[Instance]) -> None:
        fingerprint = tuple(sorted(
            (i.instance_id, max(0, i.weight))
            for i in candidates if i.weight > 0
        ))
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

    def pick(
        self,
        candidates: list[Instance],
        key: str | None = None,
        exclude: frozenset[str] | None = None,
    ) -> RouteDecision:
        exclude = exclude or frozenset()
        weighted = [i for i in candidates if i.weight > 0]
        pool = [i for i in weighted if i.instance_id not in exclude]
        if not pool:
            return self._none(key, "no candidate after excluding tried instances")
        self._build_ring(weighted)
        with self._lock:
            ring = self._ring
        if key is None:
            # 无键时退化为环上第一个未排除实例
            for _, iid in ring:
                if iid not in exclude:
                    return RouteDecision(key=None, instance_id=iid,
                                         strategy=self.name, reason="no key, ring head")
            return self._none(key, "no candidate after excluding tried instances")
        h = _hash(key)
        start = bisect.bisect_left(ring, (h,))
        n = len(ring)
        # 沿顺时针找第一个未被排除的属主
        for step in range(n):
            _, iid = ring[(start + step) % n]
            if iid not in exclude:
                return RouteDecision(key=key, instance_id=iid, strategy=self.name,
                                     reason=f"ring slot {(start + step) % n}/{n}")
        return self._none(key, "no candidate after excluding tried instances")
