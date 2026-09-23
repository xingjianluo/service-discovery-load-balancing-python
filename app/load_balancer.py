"""择路策略：轮询、最少在途、按键一致性哈希。

一致性哈希使用虚拟节点环（MD5 哈希，每实例 vnodes 个副本）：
- 候选集合不变时，同一 key 始终落在同一实例；
- 增加/删除一个实例，理论上只有约 1/N 的键发生迁移。
"""
from __future__ import annotations

import bisect
import hashlib
import threading

from .types import BalancingStrategy, Instance


def _hash(value: str) -> int:
    return int.from_bytes(hashlib.md5(value.encode()).digest(), "big")


class _ConsistentHashRing:
    def __init__(self, vnodes: int = 160) -> None:
        self._vnodes = vnodes
        self._sorted_keys: list[int] = []
        self._owners: list[str] = []        # 与 _sorted_keys 平行
        self._fingerprint: tuple[str, ...] = ()

    def get_owner(self, instances: list[Instance], key: str) -> str | None:
        if not instances:
            return None
        fingerprint = tuple(sorted(i.id for i in instances))
        if fingerprint != self._fingerprint:
            self._rebuild(instances)
            self._fingerprint = fingerprint
        h = _hash(key)
        pos = bisect.bisect_right(self._sorted_keys, h)
        if pos == len(self._sorted_keys):
            pos = 0
        return self._owners[pos]

    def _rebuild(self, instances: list[Instance]) -> None:
        ring: list[tuple[int, str]] = []
        for inst in instances:
            for v in range(self._vnodes):
                ring.append((_hash(f"{inst.id}#{v}"), inst.id))
        ring.sort(key=lambda item: item[0])
        self._sorted_keys = [h for h, _ in ring]
        self._owners = [owner for _, owner in ring]


class LoadBalancer:
    def __init__(self, strategy: BalancingStrategy, vnodes: int = 160) -> None:
        self._strategy = strategy
        self._lock = threading.Lock()
        self._rr_index = 0
        self._ring = _ConsistentHashRing(vnodes=vnodes)

    @property
    def strategy(self) -> BalancingStrategy:
        return self._strategy

    def choose(
        self,
        candidates: list[Instance],
        key: str | None = None,
        inflight: dict[str, int] | None = None,
    ) -> Instance | None:
        if not candidates:
            return None
        ordered = sorted(candidates, key=lambda i: i.id)
        by_id = {i.id: i for i in ordered}
        if self._strategy is BalancingStrategy.ROUND_ROBIN:
            return self._choose_round_robin(ordered)
        if self._strategy is BalancingStrategy.LEAST_INFLIGHT:
            return self._choose_least_inflight(ordered, inflight or {})
        return by_id.get(
            self._ring.get_owner(ordered, key if key is not None else "")
        )

    def _choose_round_robin(self, ordered: list[Instance]) -> Instance:
        # 加权展开：weight=2 的实例在一个周期内出现两次。
        wheel: list[Instance] = []
        for inst in ordered:
            wheel.extend([inst] * max(inst.weight, 1))
        with self._lock:
            chosen = wheel[self._rr_index % len(wheel)]
            self._rr_index = (self._rr_index + 1) % len(wheel)
        return chosen

    @staticmethod
    def _choose_least_inflight(
        ordered: list[Instance], inflight: dict[str, int]
    ) -> Instance:
        # 主排序键为在途数，次排序键为实例 id，保证并发下结果确定。
        return min(
            ordered,
            key=lambda i: (inflight.get(i.id, 0), i.id),
        )
