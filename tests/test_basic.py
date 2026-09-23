"""占位测试，骨架阶段验证工程可导入。"""


def test_import() -> None:
    from app import types  # noqa: F401

    assert types.BalancingStrategy.ROUND_ROBIN == "round_robin"
