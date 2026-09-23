# 服务注册发现与流量调度（本地可验证）

在本地用进程内模拟的上游实例，实现一套可验证的服务注册发现与负载均衡能力：
实例健康状态、并发在途请求数与路由结果保持一致且可解释。

## 架构

```
sd/
  models.py     数据模型：Instance / HealthState / UnavailableReason / FailureKind / RouteDecision / CallResult
  registry.py   ServiceRegistry：注册、摘除、健康收敛、在途计数（全部在锁内，线程安全）
  router.py     三种择路策略：RoundRobin / LeastInflight / ConsistentHash（虚拟节点环）
  breaker.py    CircuitBreaker：closed / open / half_open 三态熔断
  simulator.py  SimulatedUpstream：本地模拟上游，可注入失败、延迟、挂起
  balancer.py   LoadBalancer：择路 + 熔断过滤 + 有界重试的故障转移
  health.py     HealthChecker：后台周期探测，驱动健康状态收敛
main.py         FastAPI 本地验证入口
tests/          pytest 测试（摘除恢复、键亲和、并发重试）
```

## 调度策略

| 策略 | 行为 | 稳定性保证 |
|---|---|---|
| `round_robin` | 候选按 id 排序后游标取模，均匀轮转 | 游标单调递增，顺序可预期 |
| `least_inflight` | 选在途请求数最小者 | 并列时按 id 字典序，结果确定 |
| `consistent_hash` | 每实例 100 个虚拟节点构成哈希环，按键顺时针取点 | 实例集合不变时同键恒定落点；实例增减只迁移受影响键（测试断言：迁移键集 == 原落在被摘除实例上的键集） |

任何策略的候选集都只包含**健康且熔断器放行**的实例，已摘除实例不会被路由。

## 健康与熔断规则

**健康状态机**（`ServiceRegistry`，阈值默认连续失败 3 次）：

- 探测失败累计达阈值：`healthy -> unhealthy`，原因 `probe_failed`，不再参与调度。
- 探测成功：清零计数并恢复 `healthy`，重新参与调度。
- 显式摘除：`-> deregistered`，原因 `manually_removed`，**探测成功也不会复活**。
- `GET /instances` 返回每个实例的状态、不可用原因、连续失败次数、阈值与在途数。

**熔断器**（每实例一个，默认失败 3 次熔断、冷却 5s）：

- `closed`：正常放行；连续失败达阈值转 `open`。
- `open`：拒绝调用（该实例不参与择路）；冷却结束转 `half_open`。
- `half_open`：放行一次试探，成功回 `closed`，失败重新 `open`。

**重试约束（避免重试风暴）**：

- 仅 `retryable`（模拟 503）与 `timeout` 失败会转移重试，`non_retryable` 立即返回。
- 重试上限 `max_retries`（默认 2，即最多 3 次尝试），超出即返回 `retries exhausted`。
- 熔断打开的实例直接排除在候选之外，重试不会放大对故障实例的调用量。

## 并发语义

- 注册表所有状态变更（注册/摘除/健康收敛/在途增减）都在同一把 `RLock` 内完成。
- `acquire`（在途 +1）与可用性检查是**原子**的：择路选中后若实例恰被并发摘除，
  `acquire` 失败并立即重选，不会把请求送到已摘除实例。
- `release` 在 `finally` 中执行且不允许计数为负，并发压力下在途数不漂移
  （测试断言 32 线程并发后所有实例 `inflight == 0`）。
- 熔断器状态迁移各自持锁，`allow`/`on_failure` 配对不会出现竞态错算。

## 本地验证

```bash
# 运行测试（日志打印请求键、选中实例、状态变化与重试次数）
uv run pytest tests/ -q -s

# 启动本地服务
uv run uvicorn main:app --port 8000

# 注册三个实例
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"a"}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"b"}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"c"}'

# 按键路由（一致性哈希，同键恒定）
curl 'localhost:8000/route?key=user-1&strategy=consistent_hash'

# 注入故障：a 接下来 100 次调用失败
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"fail_next":100}'

# 观察健康收敛（约 3 个探测周期后 a 变为 unhealthy / probe_failed）
watch -n1 curl -s localhost:8000/instances

# 故障期间路由自动避开 a；清除故障后 a 自动恢复参与调度
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"fail_next":0}'

# 显式摘除（原因 manally_removed，不会被探测复活）
curl -X DELETE localhost:8000/instances/b

# 查看熔断器状态
curl 'localhost:8000/breakers/a?strategy=round_robin'
```
