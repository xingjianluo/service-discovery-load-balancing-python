"""传输抽象：单测用进程内直连，本地验证用真实 HTTP。"""
from __future__ import annotations

import http.client
import json
import socket
from collections.abc import Callable
from typing import Any, Protocol

from .types import (
    FatalUpstreamError,
    Instance,
    Request,
    RetryableUpstreamError,
    UpstreamTimeout,
)

Handler = Callable[[Request, Instance], Any]


class Transport(Protocol):
    """传输协议：给定实例与请求，返回响应或抛出 UpstreamError。"""

    def send(self, instance: Instance, request: Request) -> Any:
        ...


class InProcessTransport:
    """进程内传输：按实例 id 查处理器，行为可编排，结果确定，用于测试。"""

    def __init__(self, handlers: dict[str, Handler]) -> None:
        self._handlers = handlers

    def send(self, instance: Instance, request: Request) -> Any:
        handler = self._handlers.get(instance.id)
        if handler is None:
            raise RetryableUpstreamError(f"no handler for instance {instance.id}")
        return handler(request, instance)


class HttpTransport:
    """基于标准库 http.client 的真实 HTTP 传输。

    约定：
    - POST {probe_path}，body 为 {"key": ..., "payload": ...}；
    - 2xx 返回解析后的 JSON；
    - 5xx / 连接错误 / 超时视为可重试失败；
    - 4xx 视为不可重试（业务）失败。
    """

    def __init__(self, probe_path: str = "/invoke") -> None:
        self._path = probe_path

    def send(self, instance: Instance, request: Request) -> Any:
        body = json.dumps(
            {"key": request.key, "payload": request.payload}
        ).encode()
        try:
            conn = http.client.HTTPConnection(
                instance.address,
                instance.port,
                timeout=request.timeout,
            )
            try:
                conn.request(
                    "POST",
                    self._path,
                    body=body,
                    headers={"Content-Type": "application/json"},
                )
                resp = conn.getresponse()
                raw = resp.read()
                status = resp.status
            finally:
                conn.close()
        except (socket.timeout, TimeoutError) as exc:
            raise UpstreamTimeout(f"timeout calling {instance.endpoint}") from exc
        except OSError as exc:
            raise RetryableUpstreamError(
                f"connection error calling {instance.endpoint}: {exc}"
            ) from exc

        if 200 <= status < 300:
            try:
                return json.loads(raw.decode())
            except (ValueError, UnicodeDecodeError):
                return raw.decode(errors="replace")
        if status >= 500:
            raise RetryableUpstreamError(
                f"upstream {instance.endpoint} status {status}"
            )
        raise FatalUpstreamError(
            f"upstream {instance.endpoint} fatal status {status}"
        )
