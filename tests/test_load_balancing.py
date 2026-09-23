"""择路策略测试：轮询、最少在途、按键一致性哈希。"""
from __future__ import annotations

import logging

from app.load_balancer import LoadBalancer
from app.types import BalancingStrategy, Instance

logger = logging.getLogger("tests.lb")


def make_instances():
    return [
        Instance(id="a", address="h", port=1),
        Instance(id="b", address="h", port=2),
        Instance(id="c", address="h", port=3),
    ]


def test_round_robin_cycles_in_order() -> None:
    lb = LoadBalancer(BalancingStrategy.ROUND_ROBIN)
    instances = make_instances()
    picked = [lb.choose(instances, None).id for _ in range(6)]
    logger.info("轮询序列=%s", picked)
    assert picked == ["a", "b", "c", "a", "b", "c"]


def test_round_robin_weighted() -> None:
    lb = LoadBalancer(BalancingStrategy.ROUND_ROBIN)
    instances = [
        Instance(id="a", address="h", port=1, weight=2),
        Instance(id="b", address="h", port=2, weight=1),
    ]
    picked = [lb.choose(instances, None).id for _ in range(6)]
    assert picked == ["a", "a", "b", "a", "a", "b"]


def test_least_inflight_picks_lowest() -> None:
    lb = LoadBalancer(BalancingStrategy.LEAST_INFLIGHT)
    instances = make_instances()
    inflight = {"a": 3, "b": 1, "c": 2}
    picked = lb.choose(instances, "k", inflight)
    logger.info("在途 a=3 b=1 c=2 -> 选中=%s", picked.id)
    assert picked.id == "b"


def test_least_inflight_tie_breaks_by_id() -> None:
    lb = LoadBalancer(BalancingStrategy.LEAST_INFLIGHT)
    instances = make_instances()
    assert lb.choose(instances, "k", {}).id == "a"


def test_consistent_hash_stable_for_same_set() -> None:
    lb = LoadBalancer(BalancingStrategy.CONSISTENT_HASH)
    instances = make_instances()
    keys = [f"key-{i}" for i in range(100)]
    first = {k: lb.choose(instances, k).id for k in keys}
    second = {k: lb.choose(instances, k).id for k in keys}
    assert first == second
    logger.info("同实例集合下 100 个键两次择路完全一致，样例=%s",
                {k: first[k] for k in keys[:5]})


def test_consistent_hash_minimal_migration_on_remove() -> None:
    lb = LoadBalancer(BalancingStrategy.CONSISTENT_HASH)
    instances = make_instances()
    keys = [f"key-{i}" for i in range(600)]
    before = {k: lb.choose(instances, k).id for k in keys}

    reduced = [i for i in instances if i.id != "b"]
    after = {k: lb.choose(reduced, k).id for k in keys}

    moved = [k for k in keys if before[k] != after[k]]
    # 只有原本落在 b 上的键需要迁移，其余键必须原地不动。
    must_move = {k for k, owner in before.items() if owner == "b"}
    assert set(moved) == must_move
    logger.info("摘除 b：迁移键=%d/600，其余 %d 键保持原实例",
                len(moved), 600 - len(moved))
    # 三实例时迁移量约为 1/3，远少于全量。
    assert len(moved) < 400


def test_consistent_hash_minimal_migration_on_add() -> None:
    lb = LoadBalancer(BalancingStrategy.CONSISTENT_HASH)
    instances = make_instances()
    added = instances + [Instance(id="d", address="h", port=4)]
    keys = [f"key-{i}" for i in range(600)]
    before = {k: lb.choose(instances, k).id for k in keys}
    after = {k: lb.choose(added, k).id for k in keys}
    moved = [k for k in keys if before[k] != after[k]]
    logger.info("新增 d：迁移键=%d/600", len(moved))
    assert len(moved) < 400


def test_empty_candidates_returns_none() -> None:
    assert LoadBalancer(BalancingStrategy.ROUND_ROBIN).choose([], None) is None
