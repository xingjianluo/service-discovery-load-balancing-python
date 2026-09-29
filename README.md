# 服务注册发现与流量调度（本地可验证）

在本地用进程内模拟的上游实例，实现一套可验证的服务注册发现与负载均衡能力：
实例健康状态、并发在途请求数与路由结果保持一致且可解释。

## 架构

```
sd/
  models.py     数据模型：Instance / HealthState / UnavailableReason / FailureKind / RouteDecision / CallResult
  registry.py   ServiceRegistry：注册、摘除、权重热调整、优雅排水、容量与在途计数（全部在锁内，线程安全）
  router.py     三种加权择路策略：平滑加权轮询 / 加权最少在途 / 加权一致性哈希（虚拟节点环）
  breaker.py    CircuitBreaker：closed / open / half_open 三态熔断
  simulator.py  SimulatedUpstream：本地模拟上游，可注入失败、延迟、挂起
  balancer.py   LoadBalancer：择路 + 熔断/容量过滤 + 有界重试（强制换实例）的故障转移
  health.py     HealthChecker：后台周期探测，驱动健康状态收敛并推进排水超时落地
main.py         FastAPI 本地验证入口
tests/          pytest 测试（摘除恢复、键亲和、并发重试、权重、排水、容量保护）
```

## 调度策略（均感知权重）

每个实例带一个整数 `weight`（默认 1，行为与升级前一致；显式设为 0 则不参与分配新流量）。
权重可随时通过 `PUT /instances/{id}/weight` 热调整，立即生效、无需重启，流量平滑过渡。

| 策略 | 行为 | 稳定性保证 |
|---|---|---|
| `round_robin` | 平滑加权轮询（nginx 风格 current-weight）：每轮各实例累加自身权重，选当前值最大者并减去总权重 | 权重相同时退化为均匀轮询；不同权重下在一个 `Σweight` 周期内严格按比例分配，无突刺 |
| `least_inflight` | 选 `inflight / weight` 最小者（大权重摊薄在途压力） | 并列时按 id 字典序，结果确定 |
| `consistent_hash` | 每实例 `100 × weight` 个虚拟节点构成哈希环，按键顺时针取点 | 实例集合与权重不变时同键恒定；实例增减或权重变化只迁移受影响键（权重恢复的测试断言：迁移键全部落到该实例，其余键不动） |

任何策略的候选集都只包含**健康、权重大于 0、熔断器放行且容量有余量**的实例。

## 权重语义

- 注册时通过 `weight` 指定；缺省为 1，与旧行为完全一致。
- 热调整：`PUT /instances/{id}/weight {"weight": N}`，下一请求即按新权重分配。
- 权重调成 0：实例立刻不再接**新**请求（`reason=weight_zero`），但**已在途请求不受影响**，正常跑完并归还在途计数；调回正值即恢复调度。
- `GET /instances` 的 `weight` 字段返回当前权重；实例不参与调度的具体原因见 `state`/`reason`。

## 优雅下线（排水）

`POST /instances/{id}/drain {"timeout": 秒}` 触发，状态机：

1. `healthy -> draining`（`reason=draining`）：从此刻起不接新请求，候选集排除该实例；已在途请求继续正常执行。
2. 当**在途请求全部结束**（最后一次 `release` 时检测）**或超过排水时限**（健康检查线程或 `finalize_drains()` 推进），落地为
   `deregistered`（`reason=drained`），与排水中状态明确区分。`status` 里的 `drain_remaining` 表示剩余排水秒数。
3. 重复/并发触发同一实例的排水是**幂等**的：不会重置截止时间、不会提前摘除、不会算错在途数。
4. 排水中的实例不会被健康探测复活；排水超时落地时若仍有在途请求，这些请求仍会跑完，`release` 只把计数归还到 0，不会变负。
5. 想恢复流量需重新注册。直接 `DELETE`（`manually_removed`）的硬摘除语义保持不变。

## 实例级并发容量保护

- 注册时 `max_inflight`（或 `PUT /instances/{id}/capacity`）设置每实例最大并发，`0` 表示不限（默认）。
- 择路候选自动排除已达上限的实例，请求分流到其他有余量的实例。
- **状态查询里容量打满是明确的不可用原因**：健康实例在途达到上限时
  `GET /instances` 直接返回 `reason=capacity_full`（同时 `at_capacity=true`、
  `state` 仍为 `healthy`），不再只看到一个容量标记却显示"正常"。该原因是按
  当前在途/上限**实时派生**的，不写回实例持久原因，因此：
  - 在途请求归还（`release`）后立刻恢复成 `reason=none`，可重新接流；
  - 热调大/调小上限（或设回 `0` 不限）后原因同步更新，无需重启；
  - 健康异常（`probe_failed`）、权重为 0（`weight_zero`）、排水（`draining`/`drained`）、
    硬摘除（`manually_removed`）等持久原因优先展示，绝不与容量原因互相覆盖或混淆。
- 容量检查与在途 `+1` 在注册表同一把锁内原子完成，并发压力下绝不超发（测试断言：32 线程、每实例容量 2，观测到的峰值在途 ≤ 2）。
- **所有可用实例都到上限时，请求立即失败返回**（不排队、不空转、不会反复打到同一实例、
  不做任何重试），失败类型为专门的 `failure_kind=capacity_exhausted`
  （`detail="all instances at capacity (max_inflight reached)"`），而**不是**
  `retryable`：调用方据此做降级/退避，避免把瞬时容量耗尽放大成重试风暴。
  熔断打开、健康异常等其它"无候选"情况仍归 `retryable`。容量耗尽的失败不触碰任何
  上游、不改变熔断器状态、不消耗重试次数（`attempts=1`），在途归还后原因与计数同时归位。

**不可用原因对照**（`reason` 字段）：

| reason | 含义 | 是否还会自动恢复 |
|---|---|---|
| `none` | 正常 | — |
| `probe_failed` | 连续健康探测失败 | 探测成功后恢复 |
| `circuit_open` | 熔断器打开（该原因体现在择路排除，不写回实例） | 冷却后半开试探 |
| `weight_zero` | 权重为 0 | 权重调正后恢复 |
| `draining` | 排水中，在途仍在跑 | 在途清零/超时后落地，不自动恢复 |
| `drained` | 排水完成已落地 | 需重新注册 |
| `manually_removed` | 显式硬摘除 | 需重新注册 |
| `capacity_full` | 并发在途达到上限 | 有在途请求结束后立即可用 |

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
- `capacity_exhausted`（全部实例并发打满）**不重试**：立即返回且不触碰任何实例，
  调用方应退避/降级，从根上避免重试风暴。
- **每次重试强制换到另一个实例**：本次调用已失败（或已打满/已摘除）的实例记入排除集，三种策略都不会反复重试同一个坏实例（测试覆盖三种策略的 503 与超时转移）。
- 重试上限 `max_retries`（默认 2，即最多 3 次尝试），超出即返回 `retries exhausted`；实例数不足时提前结束。
- 熔断打开的实例直接排除在候选之外，重试不会放大对故障实例的调用量。

## 并发语义

- 注册表所有状态变更（注册/摘除/权重调整/健康收敛/排水/在途增减）都在同一把 `RLock` 内完成。
- 容量检查、可用性检查与 `acquire`（在途 +1）是**原子**的：择路选中后若实例恰被并发摘除、调 0 权重或打满容量，
  `acquire` 失败并立即换实例重选，不会把请求送到已摘除/已满实例，也不消耗重试次数。
- `release` 在 `finally` 中执行且不允许计数为负；若实例正处于排水且本次释放后在途清零，则同锁内立刻落地为 `drained`。
- 熔断器状态迁移各自持锁，`allow`/`on_failure` 配对不会出现竞态错算。

**与既有规则的兼容范围**：不显式设置权重（默认 1）与容量（默认不限）时，三种策略的分配结果与升级前一致；
健康状态机、熔断三态、有界重试、在途计数归零等既有用例全部保留并持续断言；权重 0、排水、容量打满都只影响
"是否接新请求"，从不中断或丢弃在途请求。

## 本地验证

```bash
# 运行测试（日志打印请求键、选中实例、状态变化与重试次数）
uv run pytest tests/ -q -s

# 启动本地服务
uv run uvicorn main:app --port 8000

# 注册三个实例（带权重与并发容量，缺省 weight=1、max_inflight=0 即不限）
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"a","weight":1,"max_inflight":4}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"b","weight":1}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"c","weight":1}'

# 热调整权重（灰度：新版本小权重 -> 逐步调大），无需重启
curl -X PUT localhost:8000/instances/a/weight -H 'content-type: application/json' -d '{"weight":3}'
curl -X PUT localhost:8000/instances/a/weight -H 'content-type: application/json' -d '{"weight":0}'  # 立刻停新流量
curl -X PUT localhost:8000/instances/a/weight -H 'content-type: application/json' -d '{"weight":1}'  # 恢复

# 热调整并发容量上限
curl -X PUT localhost:8000/instances/b/capacity -H 'content-type: application/json' -d '{"max_inflight":2}'

# —— 复现“容量打满”边界 ——
# 1) 只留 a、b 两个实例并把各自并发上限设为 1，再让调用挂住（latency 超过 0.5s 超时）
curl -X DELETE localhost:8000/instances/c
curl -X PUT localhost:8000/instances/a/capacity -H 'content-type: application/json' -d '{"max_inflight":1}'
curl -X PUT localhost:8000/instances/b/capacity -H 'content-type: application/json' -d '{"max_inflight":1}'
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"latency":2}'
curl -X POST localhost:8000/instances/b/fault -H 'content-type: application/json' -d '{"latency":2}'
# 2) 先后各发起一个占用在途的慢调用（后台挂起），此时 a、b 都打满
curl 'localhost:8000/route?key=hold-a&strategy=round_robin' &
curl 'localhost:8000/route?key=hold-b&strategy=round_robin' &
sleep 1
# 3) 状态查询：a、b 的 reason 明确为 capacity_full（state 仍是 healthy）
curl -s localhost:8000/instances | python3 -m json.tool
# 4) 再来一个调用：立即返回 failure_kind=capacity_exhausted、attempts=1（非 retryable，不重试不空等）
curl 'localhost:8000/route?key=overflow&strategy=round_robin'
# 5) 等慢调用超时归还（约 0.5s 后），reason 自动恢复 none；或热调大上限立即恢复
curl -X PUT localhost:8000/instances/a/capacity -H 'content-type: application/json' -d '{"max_inflight":0}'
curl -s localhost:8000/instances | python3 -m json.tool

# 优雅下线：排水 10 秒，在途清零或超时后落地（state: draining -> deregistered / reason: drained）
curl -X POST localhost:8000/instances/b/drain -H 'content-type: application/json' -d '{"timeout":10}'

# 查看权重、容量、在途、排水剩余时间与不可用原因
curl -s localhost:8000/instances | python3 -m json.tool

# 按键路由（加权一致性哈希，同键恒定；权重/成员变化只迁移受影响键）
curl 'localhost:8000/route?key=user-1&strategy=consistent_hash'

# 注入故障：a 接下来 100 次调用失败
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"fail_next":100}'

# 观察健康收敛（约 3 个探测周期后 a 变为 unhealthy / probe_failed）
watch -n1 curl -s localhost:8000/instances

# 故障期间路由自动避开 a；清除故障后 a 自动恢复参与调度
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"fail_next":0}'

# 显式硬摘除（原因 manually_removed，不会被探测复活）
curl -X DELETE localhost:8000/instances/b

# 查看熔断器状态
curl 'localhost:8000/breakers/a?strategy=round_robin'
```

新增能力的自动化验证：

```bash
uv run pytest tests/test_weight_drain_capacity.py -v -s
# 权重比例与热调整、权重 0、三种策略的加权择路；
# 排水在途跑完落地、排水超时落地、重复/并发排水幂等、探测不复活；
# 容量打满分流、全满快速失败、32 线程不超发；
# 单实例打满 reason=capacity_full、全满 failure_kind=capacity_exhausted；
# 容量释放后原因与在途同时归位、上限热调整即时更新、容量原因不掩盖权重0/排水/健康异常；
# 三种策略下 503/超时都强制换到别的实例。
```
