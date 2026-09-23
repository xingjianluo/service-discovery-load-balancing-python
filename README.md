# 服务注册发现与流量调度（本地可验证）

用本地进程模拟多个上游实例，提供实例注册/摘除、健康状态收敛、三种择路策略、
故障转移、熔断与并发安全的调度能力，并保证**实例健康状态、并发在途请求与
路由结果一致且可解释**。

## 目录结构

```
app/
  types.py            # 公共类型：实例/请求/响应/状态枚举/异常/不可用原因
  registry.py         # 注册中心：注册、摘除、健康收敛、在途计数（线程安全）
  health.py           # 后台主动健康探测：连续失败摘除、恢复后回归
  circuit_breaker.py  # 每实例熔断器：CLOSED / OPEN / HALF_OPEN
  load_balancer.py    # 轮询 / 最少在途 / 按键一致性哈希（虚拟节点环）
  transport.py        # Transport 抽象：InProcessTransport / HttpTransport
  dispatcher.py       # 调度器：择路+在途+故障转移+重试预算+熔断
  upstream.py         # 本地模拟上游：可编排行为 + 线程化真实 HTTP 服务
main.py               # FastAPI 网关：管理面 + 代理面
demo_local.py         # 进程内确定性演示（5 个场景，无需端口）
demo_http.py          # 真实 HTTP 多实例端到端演示（含健康下线/恢复）
tests/                # 27 个单测/E2E/并发测试
```

## 快速开始

```bash
uv sync                       # 安装依赖（含 pytest）

# 1) 进程内确定性演示（推荐先跑，断言内置于场景中）
uv run python demo_local.py

# 2) 真实 HTTP 多实例演示（启动 3 个模拟上游 + 主动健康探测）
uv run python demo_http.py

# 3) 完整测试（实时日志打印 请求键/选中实例/状态变化/重试次数）
uv run pytest -v

# 4) 以 FastAPI 网关形式手动验证
uv run uvicorn main:app --port 8080
```

### 网关手动验证步骤

```bash
# 先在 19501/19502/19503 启动模拟上游（可参考 demo_http.py 中的用法）
# 注册实例
curl -X POST localhost:8080/admin/instances -H 'Content-Type: application/json' \
  -d '{"id":"a","address":"127.0.0.1","port":19501}'
curl -X POST localhost:8080/admin/instances -H 'Content-Type: application/json' \
  -d '{"id":"b","address":"127.0.0.1","port":19502}'
curl -X POST localhost:8080/admin/instances -H 'Content-Type: application/json' \
  -d '{"id":"c","address":"127.0.0.1","port":19503}'

# 查询健康/熔断/在途状态（判定依据可查询）
curl -s localhost:8080/admin/instances | python3 -m json.tool

# 发起调度（默认按 key 一致性哈希）
curl -X POST localhost:8080/invoke -H 'Content-Type: application/json' \
  -d '{"key":"user-42"}'

# 人工摘除 b，之后请求不再落到 b
curl -X DELETE localhost:8080/admin/instances/b
```

## 调度策略（`app/load_balancer.py`）

| 策略 | 行为 | 稳定性保证 |
| --- | --- | --- |
| `round_robin` | 按实例 id 排序后在加权展开轮上轮转（`weight=2` 一周期内被选 2 次） | 共享单调游标，临界区内推进，序列确定 |
| `least_inflight` | 选择 `(当前在途数, 实例id)` 最小者 | 次排序键为 id，并发下平局也有确定结果 |
| `consistent_hash` | MD5 虚拟节点环（默认每实例 160 个 vnode），key 顺时针归属于首个 vnode | 候选集合不变时同 key 恒落同一实例；增删实例只迁移约 `1/N` 的键（测试验证：摘除某实例时仅原归属它的键迁移） |

重试时已尝试过的实例会被加入排除集合，在剩余候选上重新择路：
- 轮询：顺延到下一个实例；
- 一致性哈希：在「去掉失败实例」的环上取新 owner，因此故障转移结果同样稳定可预期。

## 健康规则（`app/registry.py` + `app/health.py`）

实例状态机（注册表视角）：

```
register ─► HEALTHY ──连续 failure_threshold 次探测失败──► UNHEALTHY(reason=probe_failed)
              ▲                                              │
              └──── 连续 success_threshold 次探测成功 ───────┘
   HEALTHY/UNHEALTHY ──deregister()──► DEREGISTERED(reason=deregistered)
   DEREGISTERED ──重新 register()──► HEALTHY（探测不会自动恢复人工摘除）
```

- 新注册实例默认 `HEALTHY`，随后由后台探测持续校验；
- `UNHEALTHY` 实例继续被探测，恢复后自动重新参与调度；
- `DEREGISTERED` 实例不参与探测、不能被探测自动恢复、不能占用在途名额；
- 所有状态变化都带**可区分原因**（`UnavailableReason`）：
  `deregistered` / `probe_failed` / `circuit_open` /
  `retryable_upstream_error` / `timeout` / `no_candidate` / `retries_exhausted`。

`GET /admin/instances` 或 `registry.view(id)` 可查到：状态、原因、连续失败数、
最后探测时间、在途数、熔断状态与熔断截止时间。

## 熔断规则（`app/circuit_breaker.py`）

每实例独立、按**数据面请求结果**计数（与主动健康探测相互独立）：

```
CLOSED ──连续失败达 failure_threshold──► OPEN
OPEN   ──经过 open_duration，首个请求进入──► HALF_OPEN（限流 half_open_max_calls 个探测）
HALF_OPEN ──成功──► CLOSED ；──失败──► OPEN（重新计时）
```

- OPEN 期间调度器**直接跳过**该实例（快速失败），不会向其发请求，避免重试风暴；
- 熔断器的 `allow()/record_*` 均在锁内完成，与在途占位共用调度临界区。

## 故障转移与重试约束（`app/dispatcher.py`）

- 上游分类：连接错误/5xx/超时 → 可重试（`RetryableUpstreamError`/`UpstreamTimeout`）；
  4xx → 不可重试（`FatalUpstreamError`），立即终止；
- 重试次数受 `RetryConfig.max_retries` 硬约束，且同一请求不会重复打到同一实例；
- 每次失败都计入熔断器，熔断打开后该实例退出候选池；
- 全部候选耗尽时返回结构化 `DispatchReport(success=False, reason=...)`，
  `dispatch()` 则抛出 `AllInstancesUnavailable`，原因归类为
  `retries_exhausted`（有过尝试）或 `no_candidate`（无可用实例）。

## 并发语义（为什么不会错算/漂移）

1. **择路与占位原子化**：「取健康候选 → 剔除熔断 OPEN/本次已试实例 → 择路
   → 在途计数 +1」在调度器同一把 `_select_lock` 临界区内完成。实例被选中的
   瞬间其在途数即对其他线程可见，因此：
   - 最少在途策略不会把多个并发请求超发到同一个实例（测试：32 并发 / 4 实例，
     在屏障同步下严格 8/8/8/8，单实例峰值在途恰为 8）；
   - 已摘除/熔断的实例永远不会被选中。
2. **锁外调用**：真正的上游 HTTP 调用在锁外执行，不阻塞其他请求择路；
   无论成功、失败还是异常，名额都会释放。
3. **状态一致**：注册表用单一 `RLock` 保护全部可变状态；熔断器用独立锁
   保护状态迁移。并发测试（120 个全失败请求 / 24 线程）结束后所有实例
   在途计数必须归零，熔断终态一致。
4. **路由确定**：轮询游标临界区推进；最少在途用 id 作平局键；一致性哈希
   环在候选指纹不变时复用，同 key 在并发下零漂移。

## 测试与可观测日志

```bash
uv run pytest -v
```

- `test_registry_health.py`：注册/摘除、探测连续失败收敛、恢复回归、摘除不被探测；
- `test_load_balancing.py`：轮询/加权轮询、最少在途、一致性哈希亲和与增删迁移率；
- `test_dispatch_failover.py`：可重试失败/超时转移、重试预算上限、不可重试不重试、
  熔断打开跳过、半开恢复、原因归类；
- `test_concurrency.py`：并发在途均衡、并发下哈希零漂移、全失败后在途归零；
- `test_http_e2e.py`：真实 HTTP 上游的轮询/摘除/健康收敛恢复/500 转移/连接拒绝转移。

调度器与健康检查器通过标准 `logging` 输出，pytest 已开启实时日志，每条关键
日志包含 **请求 key、选中实例、状态变化与重试次数**，例如：

```
INFO scheduler.dispatcher: key=order-1 attempt=1 selected=a strategy=round_robin
WARNING scheduler.dispatcher: key=order-1 instance=a failed reason=retryable_upstream_error retryable=True
INFO scheduler.dispatcher: key=order-1 retry=1/2 failover
INFO scheduler.dispatcher: key=order-1 success instance=c attempts=2 retries=1
WARNING scheduler.health: instance=b state -> unhealthy reason=probe_failed
```

## 本地模拟上游（`app/upstream.py`）

- `UpstreamBehavior`：可编排前 N 次 500/504/400、持续失败、延迟、健康开关；
- `make_handler(behavior)`：进程内 `InProcessTransport` 用处理器，计数线程安全；
- `LocalUpstreamServer`：线程化真实 HTTP 服务，提供
  - `GET /health`（200/503）
  - `POST /invoke`（200/500/504/400，行为由 `UpstreamBehavior` 驱动）
  - `POST /admin/health` body `{"healthy": false}`：演示故障注入与恢复。
