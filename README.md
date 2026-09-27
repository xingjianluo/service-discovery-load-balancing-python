# 服务注册发现与流量调度（本地可验证）

在本地用进程内模拟的上游实例，实现一套可验证的服务注册发现与负载均衡能力：
实例健康状态、并发在途请求数与路由结果保持一致且可解释。在基础的注册摘除、
健康收敛、三种择路、熔断与有限重试之上，本轮新增**灰度权重、优雅排水（在线
上下线）与实例级并发容量保护**。

## 架构

```
sd/
  models.py     数据模型：Instance（含 weight / max_concurrency / 排水状态）/ HealthState / UnavailableReason / FailureKind / RouteDecision / CallResult
  registry.py   ServiceRegistry：注册、摘除、健康收敛、权重/容量热调、优雅排水、在途计数（全部在锁内，线程安全）
  router.py     三种加权择路策略：平滑加权 RoundRobin / 加权 LeastInflight / 加权 ConsistentHash（虚拟节点环）
  breaker.py    CircuitBreaker：closed / open / half_open 三态熔断
  simulator.py  SimulatedUpstream：本地模拟上游，可注入失败、延迟、挂起、在途闸门
  balancer.py   LoadBalancer：择路 + 熔断/权重/容量过滤 + 有界重试的故障转移（不复试同一实例）
  health.py     HealthChecker：后台周期探测，驱动健康状态收敛
main.py         FastAPI 本地验证入口（权重/容量热调、排水接口）
tests/          pytest 测试（摘除恢复、键亲和、并发重试、权重、排水、容量）
```

## 三种择路策略（均已权重化）

| 策略 | 行为 | 权重如何参与 |
|---|---|---|
| `round_robin` | 平滑加权轮询（Nginx SWRR）：每轮累计权重、选累计值最大者后回减总权重 | 选中次数与权重成比例且**交错平滑**；权重相等时退化为按 id 字典序均匀轮转，与旧行为一致 |
| `least_inflight` | 选“在途/权重”占用率最低者 | 高权重实例在相同在途下占用率更低、优先被选；权重相等时即纯最少在途 |
| `consistent_hash` | 每实例 `100×权重` 个虚拟节点构成哈希环，按键顺时针取点 | 权重越大环上节点越多、占比越高；同键恒定落点，实例/权重变化只迁移受影响键 |

- 权重在注册时可指定，运行中可随时热调整（`POST /instances/{id}/weight`），**无需重启**，下一轮择路立即生效。
- 权重默认 `1`；不显式指定时分布与改造前完全一致。
- 权重调为 `0`：该实例**立即不再接新请求**，但已接到的在途请求不受影响、正常跑完并归还在途计数；状态查询原因为 `weight_zero`。调回正值即恢复。
- 故障转移时三种策略都会**排除本次调用已试过的实例**：一致性哈希沿环顺时针找下一个属主，不会反复落在同一个坏实例。

## 优雅排水（在线上下线，不中断在途请求）

`POST /instances/{id}/drain {"drain_timeout": 秒}` 让实例进入排水：

1. 状态变为 `draining`（`scheduling_reason=draining`），从那一刻起**不接任何新请求**。
2. **已经在途的请求正常跑完**，完成后把在途计数还回去（`release` 在 `finally` 中执行）。
3. 满足以下任一条件才真正落地为不可用 `drained`（`scheduling_reason=drained`）：
   - 在途请求全部结束（在途计数归零，立即落地）；
   - 超过设定的排水时限 `drain_timeout`（超时后落地，但不强制杀在途，剩余在途计数保留到真正结束时归还，不被提前清零）。
4. 排水中（`draining`）与排水结束（`drained`）是两个可区分状态；`GET /instances` 还返回 `drain_remaining_seconds`。
5. **幂等且并发安全**：重复/并发触发同一个实例的排水，只有第一次生效，不会刷新时限、不会提前摘走，也不会算错在途数（测试断言 8 线程并发触发，仅 1 次生效、在途不变）。
6. 排水落地后想重新上线，重新 `POST /instances` 注册即可（健康/排水/在途等运行态全部复位）。

> 若希望“摘除但不中断在途”，推荐先 drain 再 deregister；`DELETE` 仍是立即标记 `deregistered`，
> 但同样不会打断已在途的请求（它们仍会执行完并归还计数）。

## 实例级并发容量上限

- 注册时可用 `max_concurrency` 指定，运行中可热调整（`POST /instances/{id}/concurrency`）；`0` 表示不限（默认）。
- 实例在途数达到上限即被视为**暂时不可用**，调度把流量分到其它还有余量的实例；
  容量判定与在途 +1 在注册表锁内**原子**完成，并发压力下绝不超发（测试断言每实例在途 ≤ 容量）。
- 所有实例都到上限时，请求**尽快失败**返回，不干等、不堆积：
  `failure_kind = capacity_exhausted`、`detail = all instances at concurrency capacity`，且尝试次数为 1。
- 容量是软边界语义：热调小到低于当前在途不会强杀在途，只是在在途回落到新上限之前不再接新请求。

### 容量上限 / 不可用原因对照（`GET /instances` 的 `scheduling_reason`）

| scheduling_reason | 含义 | 还接新请求吗 |
|---|---|---|
| `none` | 健康、权重>0、未到容量 | 接 |
| `weight_zero` | 权重被调成 0 | 不接（在途继续跑完） |
| `draining` | 正在排水 | 不接（在途继续跑完） |
| `drained` | 排水结束已落地 | 不接 |
| `probe_failed` | 连续健康探测失败 | 不接，探测恢复后自动回来 |
| `manually_removed` | 被显式 DELETE 摘除，探测也不会复活 | 不接 |
| `circuit_open` | 该实例熔断器打开（见 `/breakers`） | 不接，冷却/试探后恢复 |
| `at_capacity` | 在途已达实例并发上限 | 暂时不接，有余量即恢复 |

状态查询同时返回每个实例的 `weight`、`max_concurrency`、`inflight`、`state`、`reason`、`scheduling_reason`。

## 健康、熔断、重试与新规则的兼容范围

- **健康状态机**（`ServiceRegistry`，阈值默认连续失败 3 次）：探测失败累计达阈值
  `healthy -> unhealthy(probe_failed)`；探测成功清零并恢复；显式摘除为
  `deregistered(manually_removed)`，**探测成功不会复活**；排水中的实例同样不被探测结果改变状态。
- **熔断器**（每实例一个，默认失败 3 次熔断、冷却 5s）：`closed/open/half_open` 规则不变；
  熔断打开的实例与权重 0、排水中、到容量的实例一起被排除在候选之外。
- **重试约束**：仅 `retryable`（503）与 `timeout` 转移重试，`non_retryable` 立即返回；
  最多 `1 + max_retries` 次尝试；**同一次调用不会复试同一个实例**，
  重试次数、熔断状态、在途计数均不因权重/排水/容量逻辑漂移（并发测试断言收尾在途全为 0）。
- **容量耗尽不计入重试**：全体到上限时直接 `capacity_exhausted` 快速失败，避免无谓重试放大压力。

## 并发语义

- 注册表所有状态变更（注册/摘除/健康收敛/权重容量调整/排水/在途增减）都在同一把 `RLock` 内完成。
- 候选快照、容量判定与 `acquire`（在途 +1）原子化：选中后若实例恰被并发置权重 0/排水/摘走/打满，
  `acquire` 带原因失败并立即改选其它实例，不会超发也不会把请求送到不可用实例。
- `release` 在 `finally` 中执行、不允许计数为负；排水实例在途归零的瞬间落地为 `drained`。
- 熔断器状态迁移各自持锁，`allow`/`on_failure` 配对不会竞态错算。

## 本地验证

```bash
# 运行全部测试（含权重分配/热调整、排水在途完成与超时落地、容量不超发、故障转移不回归）
uv run pytest tests/ -q -s

# 启动本地服务
uv run uvicorn main:app --port 8000

# 注册：c 权重 4、并发上限 2；a/b 默认权重 1、不限并发
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"a"}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"b"}'
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"c","weight":4,"max_concurrency":2}'

# 多次路由，观察 c 约占 4/(1+1+4) 的流量（轮询下平滑交错）
for i in $(seq 1 12); do curl -s 'localhost:8000/route?key=k'$i'&strategy=round_robin'; echo; done

# 运行中热调整权重：把 a 调到 4，无需重启
curl -X POST localhost:8000/instances/a/weight -H 'content-type: application/json' -d '{"weight":4}'
# 权重置 0：立即不接新请求（scheduling_reason=weight_zero），在途请求不受影响
curl -X POST localhost:8000/instances/a/weight -H 'content-type: application/json' -d '{"weight":0}'

# 给 a 设置并发上限 1，再制造并发即可观察请求被分流到 b/c；全体到上限时返回 capacity_exhausted
curl -X POST localhost:8000/instances/a/concurrency -H 'content-type: application/json' -d '{"max_concurrency":1}'

# 优雅下线 b：排水 10 秒；新请求不再到 b，在途跑完或超时后 state=drained
curl -X POST localhost:8000/instances/b/drain -H 'content-type: application/json' -d '{"drain_timeout":10}'

# 查看权重、容量、在途、状态与不可用原因（draining/drained 可区分）
watch -n1 curl -s localhost:8000/instances

# 排水落地后重新上线（重新注册，运行态复位）
curl -X POST localhost:8000/instances -H 'content-type: application/json' -d '{"instance_id":"b"}'

# 故障转移不回归：注入失败/挂起，三种策略都会换到其它实例继续尝试
curl -X POST localhost:8000/instances/a/fault -H 'content-type: application/json' -d '{"fail_next":1}'
curl -s 'localhost:8000/route?key=user-1&strategy=consistent_hash'
```
