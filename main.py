"""FastAPI 网关：管理面（注册/摘除/查询）+ 代理面（调度转发）。

本地启动：uv run uvicorn main:app --port 8080
- POST /admin/instances        注册实例
- DELETE /admin/instances/{id} 摘除实例
- GET  /admin/instances        查询健康/熔断/在途状态
- POST /invoke                 按策略调度转发 {"key": "...", "payload": ...}
"""
from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from app.dispatcher import Dispatcher, RetryConfig
from app.health import HealthChecker, HealthCheckConfig, http_probe
from app.load_balancer import LoadBalancer
from app.registry import InstanceRegistry
from app.transport import HttpTransport
from app.types import (
    AllInstancesUnavailable,
    BalancingStrategy,
    Instance,
    Request,
)

registry = InstanceRegistry()
breaker = CircuitBreaker(
    CircuitBreakerConfig(failure_threshold=5, open_duration=2.0)
)
registry.set_circuit_provider(
    lambda i: (breaker.state(i), breaker.open_until(i))
)
dispatcher = Dispatcher(
    registry,
    LoadBalancer(BalancingStrategy.CONSISTENT_HASH),
    breaker,
    HttpTransport(),
    RetryConfig(max_retries=2),
)
health_checker = HealthChecker(
    registry,
    http_probe("/health", timeout=0.5),
    HealthCheckConfig(interval=1.0, failure_threshold=3, success_threshold=1),
)

app = FastAPI(title="service-discovery-lb")


class InstanceIn(BaseModel):
    id: str
    address: str = "127.0.0.1"
    port: int
    weight: int = 1


class InvokeIn(BaseModel):
    key: str
    payload: object | None = None
    timeout: float = 1.0


@app.on_event("startup")
def _startup() -> None:
    health_checker.start()


@app.on_event("shutdown")
def _shutdown() -> None:
    health_checker.stop()


@app.get("/")
def read_root():
    return {"service": "service-discovery-lb", "instances": len(registry.views())}


@app.post("/admin/instances", status_code=201)
def register_instance(item: InstanceIn):
    instance = Instance(
        id=item.id, address=item.address, port=item.port, weight=item.weight
    )
    registry.register(instance)
    return {"registered": item.id}


@app.delete("/admin/instances/{instance_id}")
def deregister_instance(instance_id: str):
    registry.deregister(instance_id)
    return {"deregistered": instance_id}


@app.get("/admin/instances")
def list_instances():
    return [
        {
            "id": v.instance.id,
            "endpoint": v.instance.endpoint,
            "state": v.state.value,
            "reason": v.state_reason,
            "consecutive_failures": v.consecutive_failures,
            "inflight": v.inflight,
            "circuit": v.circuit.value,
        }
        for v in registry.views()
    ]


@app.post("/invoke")
def invoke(item: InvokeIn):
    try:
        resp = dispatcher.dispatch(
            Request(key=item.key, payload=item.payload, timeout=item.timeout)
        )
    except AllInstancesUnavailable as exc:
        return {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "instance": resp.instance_id,
        "value": resp.value,
        "attempts": resp.attempts,
        "retries": resp.retries,
    }
