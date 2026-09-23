"""本地进程模拟上游实例：可编排行为的处理器与线程化真实 HTTP 服务。"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .types import (
    FatalUpstreamError,
    Instance,
    Request,
    RetryableUpstreamError,
    UpstreamTimeout,
)


@dataclass
class UpstreamBehavior:
    """单实例的可编排行为（可在运行中修改，模拟故障注入/恢复）。"""

    fail_times: int = 0      # 前 N 次调用返回可重试失败（500）
    timeout_times: int = 0   # 前 N 次调用超时
    always_fail: bool = False
    fatal_times: int = 0     # 前 N 次调用返回不可重试失败（400）
    latency: float = 0.0
    healthy: bool = True     # /health 是否返回 200
    value: Any = "ok"
    _calls: int = field(default=0)


def make_handler(behavior: UpstreamBehavior):
    """生成 InProcessTransport 使用的处理器，线程安全地推进行为状态。"""
    lock = threading.Lock()

    def _handler(request: Request, instance: Instance) -> Any:
        with lock:
            behavior._calls += 1
            n = behavior._calls
            timeouts = behavior.timeout_times
            fails = behavior.fail_times
            fatals = behavior.fatal_times
            latency = behavior.latency

        if latency:
            time.sleep(latency)
        if n <= timeouts:
            raise UpstreamTimeout(f"{instance.id} simulated timeout #{n}")
        if n <= timeouts + fatals:
            raise FatalUpstreamError(f"{instance.id} simulated fatal #{n}")
        if n <= timeouts + fatals + fails or behavior.always_fail:
            raise RetryableUpstreamError(f"{instance.id} simulated failure #{n}")
        return {"instance": instance.id, "value": behavior.value, "seq": n}

    return _handler


class LocalUpstreamServer:
    """用线程启动的真实 HTTP 上游，提供 /health 与 /invoke，端到端验证用。"""

    def __init__(
        self,
        instance_id: str,
        port: int,
        behavior: UpstreamBehavior,
        host: str = "127.0.0.1",
    ) -> None:
        self.instance_id = instance_id
        self.port = port
        self.host = host
        self.behavior = behavior
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._invoke = make_handler(behavior)

    def start(self) -> None:
        behavior = self.behavior
        instance_id = self.instance_id
        invoke = self._invoke  # 供嵌套的 _Handler 闭包捕获

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:
                pass

            def _reply(self, status: int, payload: dict[str, Any]) -> None:
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/health":
                    if behavior.healthy:
                        self._reply(200, {"status": "ok", "id": instance_id})
                    else:
                        self._reply(503, {"status": "down", "id": instance_id})
                else:
                    self._reply(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                if self.path == "/admin/health":
                    # 演示用：{"healthy": false} 令 /health 返回 503，模拟宕机。
                    length = int(self.headers.get("Content-Length", "0"))
                    try:
                        body = json.loads(self.rfile.read(length).decode() or "{}")
                    except ValueError:
                        body = {}
                    behavior.healthy = bool(body.get("healthy", True))
                    self._reply(200, {"id": instance_id,
                                      "healthy": behavior.healthy})
                    return
                if self.path != "/invoke":
                    self._reply(404, {"error": "not found"})
                    return
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    payload = json.loads(raw.decode())
                except ValueError:
                    payload = {}
                request = Request(
                    key=str(payload.get("key", "")),
                    payload=payload.get("payload"),
                )
                instance = Instance(
                    id=instance_id,
                    address=self.server.server_address[0],
                    port=self.server.server_address[1],
                )
                try:
                    result = invoke(request, instance)
                except UpstreamTimeout:
                    self._reply(504, {"error": "timeout"})
                except FatalUpstreamError as exc:
                    self._reply(400, {"error": str(exc)})
                except RetryableUpstreamError as exc:
                    self._reply(500, {"error": str(exc)})
                else:
                    self._reply(200, result)

        self._httpd = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name=f"upstream-{instance_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
