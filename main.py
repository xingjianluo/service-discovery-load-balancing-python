"""本地验证入口：FastAPI 暴露注册、摘除、健康状态与调度接口。

启动：uv run uvicorn main:app --reload
"""
from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from sd.balancer import LoadBalancer
from sd.health import HealthChecker
from sd.models import Instance
from sd.registry import ServiceRegistry
from sd.router import ConsistentHashRouter, LeastInflightRouter, RoundRobinRouter
from sd.simulator import SimulatedUpstream

app = FastAPI(title="service-discovery-load-balancing")

registry = ServiceRegistry(failure_threshold=3)
upstreams: dict[str, SimulatedUpstream] = {}
routers = {
    "round_robin": RoundRobinRouter(),
    "least_inflight": LeastInflightRouter(),
    "consistent_hash": ConsistentHashRouter(),
}
balancers = {
    name: LoadBalancer(registry, router, upstreams, max_retries=2, call_timeout=0.5)
    for name, router in routers.items()
}
checker = HealthChecker(registry, upstreams, interval=1.0)


@app.on_event("startup")
def _startup() -> None:
    checker.start()


@app.on_event("shutdown")
def _shutdown() -> None:
    checker.stop()


class RegisterRequest(BaseModel):
    instance_id: str
    host: str = "127.0.0.1"
    port: int = 0


class FaultRequest(BaseModel):
    fail_next: int = 0
    latency: float = 0.0
    hang: bool = False


@app.post("/instances")
def register(req: RegisterRequest) -> dict:
    inst = Instance(req.instance_id, req.host, req.port or 9000 + len(upstreams))
    registry.register(inst)
    upstreams[inst.instance_id] = SimulatedUpstream(inst.instance_id)
    return {"registered": inst.instance_id, "address": inst.address}


@app.delete("/instances/{instance_id}")
def deregister(instance_id: str) -> dict:
    return {"deregistered": registry.deregister(instance_id)}


@app.get("/instances")
def status() -> list[dict]:
    """健康判定依据与当前状态（含可区分的不可用原因）。"""
    return registry.status()


@app.post("/instances/{instance_id}/fault")
def inject_fault(instance_id: str, req: FaultRequest) -> dict:
    """给本地模拟实例注入故障，验证健康收敛与故障转移。"""
    up = upstreams.get(instance_id)
    if up is None:
        return {"error": "unknown instance"}
    up.set_fail_next(req.fail_next)
    up.set_latency(req.latency)
    up.set_hang(req.hang)
    return {"fault_injected": instance_id}


@app.get("/route")
def route(key: str = "default", strategy: str = "round_robin") -> dict:
    """按指定策略发起一次（可能含重试的）调用。"""
    lb = balancers.get(strategy)
    if lb is None:
        return {"error": f"unknown strategy, choose from {list(balancers)}"}
    result = lb.call(key=key)
    return {
        "key": key,
        "strategy": strategy,
        "ok": result.ok,
        "instance": result.instance_id,
        "attempts": result.attempts,
        "failure_kind": result.failure_kind.value if result.failure_kind else None,
        "detail": result.detail,
    }


@app.get("/breakers/{instance_id}")
def breaker(instance_id: str, strategy: str = "round_robin") -> dict:
    return {"instance_id": instance_id,
            "state": balancers[strategy].breaker_state(instance_id)}
