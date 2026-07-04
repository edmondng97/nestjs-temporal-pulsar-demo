# Campaign Dispatch Demo — NestJS + Temporal + Pulsar

> **Language / 语言:** [English](#english) · [简体中文](#简体中文)

A technical architecture demo. The **campaign dispatch core** is extracted from a
production service with all business logic stripped out (the delivery action is an
empty stub). It focuses purely on the engineering: **Temporal workflow orchestration,
Pulsar message dispatch, CAS-based idempotent state machine, epoch fencing against
stale messages, pause/resume rewind, reconciliation safety net, hexagonal port
layering, and code discipline.**

---

<a name="english"></a>
## English

### Repository layout

| Part | Path | What it is |
|---|---|---|
| **Backend** | `backend/` | Runnable NestJS service — real Temporal / Pulsar / Mongo / Redis |
| **Console** | `frontend/` | Vite + React runtime console (list, dispatch/pause/resume, live SSE stream) |
| **Demo** | `demo/` | Static keynote-style presentation — open `demo/index.html`, scroll to play |
| **Docs** | `docs/` | Design specs, plans, and business use-case coverage |

### Prerequisites

- Docker (for the Temporal / Pulsar / Mongo / Redis stack)
- Node.js 20+

### Quick start

```bash
# 1. Boot dependencies (Temporal + Temporal UI + Pulsar + Mongo + Redis)
docker compose up -d

# 2. Backend — API + two Temporal workers + Pulsar consumer + reconciliation cron
cd backend
npm install
cp .env.example .env
npm run build && npm start          # or: npm run start:dev

# 3. Frontend console (new terminal)
cd frontend
npm install
npm run dev                          # console on http://localhost:5173
```

### Ports

| Service | URL | Notes |
|---|---|---|
| Backend API | http://localhost:3000 | REST + SSE |
| Frontend console | http://localhost:5173 | Vite dev server |
| Temporal UI | http://localhost:8080 | Inspect parent/child workflows |
| Pulsar admin | http://localhost:8081 | Topic / subscription stats |

### Architecture

```
POST /campaigns/:id/dispatch
   └─→ Temporal: executeCampaignWorkflow (parent)
          ├─ targeting activity            # generate N PENDING deliveries (empty data, idempotent)
          └─ startChild → campaignDispatcherWorkflow   # ParentClosePolicy.ABANDON
                 └─ dispatchPlayers activity
                      ├─ campaign CAS: PENDING→IN_PROGRESS (atomic, idempotent)
                      ├─ while: page through PENDING deliveries
                      │    ├─ check pause flag (Redis) → break
                      │    ├─ per-row CAS PENDING→IN_PROGRESS (mark-first)
                      │    └─ on win → Pulsar.send({deliveryId, epoch})
                      └─ heartbeat(processed)
                                    │
                                    ▼  Pulsar topic (Shared subscription)
                      CampaignDeliveryConsumer
                      ├─ re-read delivery (must be IN_PROGRESS, else ack-skip)
                      ├─ epoch fence: msg.epoch < currentEpoch → ack-skip
                      ├─ CAS IN_PROGRESS→SENDING (loser ack-skips)
                      ├─ deliverStub()           # empty delivery: sleep + random success/fail
                      └─ markTerminal SUCCESS/FAILED
   + Reconciliation cron: mark campaign COMPLETED once no non-terminal deliveries remain
   + Pause/Resume: pause = Redis flag; resume = clear flag + epoch++ + rewind IN_PROGRESS → PENDING
```

Delivery state machine: `PENDING → IN_PROGRESS → SENDING → SUCCESS / FAILED` (every transition via atomic CAS).

### API

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/campaigns` | Create a campaign |
| `GET`  | `/campaigns` | List campaigns (status, epoch, paused) |
| `GET`  | `/campaigns/:id` | Campaign detail + per-status delivery counts |
| `POST` | `/campaigns/:id/dispatch` | Start the parent workflow (epoch 0) |
| `POST` | `/campaigns/:id/pause` | Set the Redis pause flag (dispatcher stops early) |
| `POST` | `/campaigns/:id/resume` | Clear flag + bump epoch + rewind + relaunch |
| `SSE`  | `/campaigns/events` | Global live stream of per-delivery outcomes |

### Try it — dispatch

```bash
ID=$(curl -s -XPOST localhost:3000/campaigns | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -XPOST localhost:3000/campaigns/$ID/dispatch
watch -n2 "curl -s localhost:3000/campaigns/$ID"   # counts drain into SUCCESS/FAILED, then campaign COMPLETED
```

### Try it — pause / resume

```bash
curl -XPOST localhost:3000/campaigns/$ID/pause     # instant pause (dispatcher stops early)
curl -XPOST localhost:3000/campaigns/$ID/resume    # epoch++, stale messages fenced, orphan rows rewound & re-sent
# total deliveries stay constant (no dupes), everything reaches a terminal state + COMPLETED
```

### Tests

```bash
cd backend && npm test     # 3 suites, 7 cases: CAS state machine / Redis pause / reconciliation
```

### Design highlights

| Highlight | Detail |
|---|---|
| **Temporal parent/child workflow** | Parent runs targeting then `startChild` to dispatch; `ParentClosePolicy.ABANDON` lets the child continue after the parent finishes; `proxyActivities` configures retry/timeout explicitly |
| **CAS idempotency** | Every transition = a single `updateOne(filter:{status:from})` checking `modifiedCount`, no read-then-write; naturally dedupes Temporal retries and concurrent consumption |
| **Mark-first dispatch** | CAS-claim the row *before* sending the message, only-on-win; orphan rows from a crash are cleaned up by rewind/reconciliation |
| **Pulsar Shared subscription** | Horizontally scalable consumption; no-ack = redelivery = at-least-once, plus downstream CAS = effectively-once; never silently drops a message |
| **Epoch fence** | On pause/resume epoch++; in-flight messages with `epoch < current` are dropped at the consumer, preventing stale messages from corrupting state |
| **Pause/resume rewind** | Resume rewinds fenced IN_PROGRESS rows back to PENDING for a fresh dispatch round — no dupes, no lost rows |
| **Reconciliation safety net** | Completion is decided by an independent cron (the dispatcher may ABANDON/crash and exit early) → eventual consistency |
| **Hexagonal port layering** | The activity depends on the `ICampaignDeliveryDispatchPort` interface, not a concrete impl; interface-first + token injection |

> Note: business logic (rewards / budget / SMS·Email·Mail three-channel / recurring scheduling) has been fully stripped; the delivery action is an empty stub with random success/failure.

---

<a name="简体中文"></a>
## 简体中文

面试用技术架构 demo。从生产服务抽取 **Campaign 派发核心链路**，剥离全部业务逻辑（投递动作 = 空 stub），
聚焦展示：**Temporal 工作流编排、Pulsar 消息派发、基于 CAS 的幂等状态机、epoch 防陈旧消息栅栏、暂停/恢复 rewind、对账兜底、六边形端口分层、代码规范。**

### 仓库结构

| 部分 | 路径 | 说明 |
|---|---|---|
| **后端** | `backend/` | 真实可运行的 NestJS 服务（连真实 Temporal / Pulsar / Mongo / Redis） |
| **控制台** | `frontend/` | Vite + React 运行时控制台（列表、派发/暂停/恢复、SSE 实时事件流） |
| **演示** | `demo/` | 静态 keynote 风格演示文稿，打开 `demo/index.html`，滚动播放 |
| **文档** | `docs/` | 设计规格、实施计划、业务用例覆盖报告 |

### 前置条件

- Docker（运行 Temporal / Pulsar / Mongo / Redis 全家桶）
- Node.js 20+

### 快速开始

```bash
# 1. 起依赖全家桶（Temporal + Temporal UI + Pulsar + Mongo + Redis）
docker compose up -d

# 2. 后端（API + 两个 Temporal worker + Pulsar consumer + cron）
cd backend
npm install
cp .env.example .env
npm run build && npm start          # 或 npm run start:dev

# 3. 前端控制台（新终端）
cd frontend
npm install
npm run dev                          # 控制台在 http://localhost:5173
```

### 端口

| 服务 | 地址 | 说明 |
|---|---|---|
| 后端 API | http://localhost:3000 | REST + SSE |
| 前端控制台 | http://localhost:5173 | Vite dev server |
| Temporal UI | http://localhost:8080 | 查看父/子 workflow |
| Pulsar admin | http://localhost:8081 | 主题 / 订阅统计 |

### 架构

```
POST /campaigns/:id/dispatch
   └─→ Temporal: executeCampaignWorkflow (父)
          ├─ targeting activity            # 生成 N 条 PENDING delivery（空数据，幂等）
          └─ startChild → campaignDispatcherWorkflow   # ParentClosePolicy.ABANDON
                 └─ dispatchPlayers activity
                      ├─ campaign CAS: PENDING→IN_PROGRESS（原子幂等）
                      ├─ while: 分页拉 PENDING delivery
                      │    ├─ check pause flag (Redis) → break
                      │    ├─ 逐行 CAS PENDING→IN_PROGRESS（mark-first）
                      │    └─ 命中则 Pulsar.send({deliveryId, epoch})
                      └─ heartbeat(processed)
                                    │
                                    ▼  Pulsar topic（Shared 订阅）
                      CampaignDeliveryConsumer
                      ├─ 重读 delivery（必须 IN_PROGRESS，否则 ack-skip）
                      ├─ epoch 栅栏：msg.epoch < currentEpoch → ack-skip
                      ├─ CAS IN_PROGRESS→SENDING（输者 ack-skip）
                      ├─ deliverStub()           # 空投递：sleep + 随机成功/失败
                      └─ markTerminal SUCCESS/FAILED
   + Reconciliation cron：无非终态 delivery 时标记 campaign COMPLETED
   + Pause/Resume：pause = Redis flag；resume = clear flag + epoch++ + IN_PROGRESS 回退 PENDING
```

Delivery 状态机：`PENDING → IN_PROGRESS → SENDING → SUCCESS / FAILED`（全部走原子 CAS）。

### API

| 方法 | 端点 | 用途 |
|---|---|---|
| `POST` | `/campaigns` | 创建 campaign |
| `GET`  | `/campaigns` | 列表（状态、epoch、paused） |
| `GET`  | `/campaigns/:id` | 详情 + 各状态 delivery 计数 |
| `POST` | `/campaigns/:id/dispatch` | 启动父 workflow（epoch 0） |
| `POST` | `/campaigns/:id/pause` | 设置 Redis pause flag（dispatcher 早停） |
| `POST` | `/campaigns/:id/resume` | 清 flag + epoch++ + rewind + 重启 |
| `SSE`  | `/campaigns/events` | 全局实时逐条投递结果流 |

### 演示派发

```bash
ID=$(curl -s -XPOST localhost:3000/campaigns | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -XPOST localhost:3000/campaigns/$ID/dispatch
watch -n2 "curl -s localhost:3000/campaigns/$ID"   # counts 逐步进入 SUCCESS/FAILED，最终 campaign COMPLETED
```

### 演示暂停/恢复

```bash
curl -XPOST localhost:3000/campaigns/$ID/pause     # 立即暂停（dispatcher 早停）
curl -XPOST localhost:3000/campaigns/$ID/resume    # epoch++，旧消息被栅栏丢弃，孤儿行 rewind 重投
# 总投递数恒定（无重复），最终全部终态 + COMPLETED
```

### 测试

```bash
cd backend && npm test     # 3 套，7 用例：CAS 状态机 / Redis pause / reconciliation
```

### 设计亮点（面试讲点）

| 亮点 | 说明 |
|---|---|
| **Temporal 父子 workflow** | 父跑 targeting 后 `startChild` 派发；`ParentClosePolicy.ABANDON` 让父完成后子继续；`proxyActivities` 显式配置 retry/timeout |
| **CAS 幂等** | 所有状态转换 = 单条 `updateOne(filter:{status:from})` 看 `modifiedCount`，无读后写；Temporal 重试与并发消费天然去重 |
| **Mark-first 派发** | 先 CAS 占位再发消息，only-on-win；崩溃留下的孤儿行由 rewind/对账收尾 |
| **Pulsar Shared 订阅** | 水平扩展消费；不 ack 即重投 = at-least-once，配合下游 CAS = effectively-once；绝不静默丢消息 |
| **epoch 栅栏** | 暂停/恢复时 epoch++，在途旧消息 `epoch<current` 被消费端丢弃，防止陈旧消息污染状态 |
| **暂停/恢复 rewind** | resume 把被栅栏丢弃的 IN_PROGRESS 行回退 PENDING，新一轮重投——不重复、不丢行 |
| **对账兜底** | 完成判定交独立 cron（dispatcher 可能 ABANDON/崩溃提前退出）→ 最终一致性 |
| **六边形端口分层** | activity 依赖 `ICampaignDeliveryDispatchPort` 接口而非具体实现；interface-first + token 注入 |

> 注：业务逻辑（发奖/预算/SMS·Email·Mail 三通道/recurring 定时）已全部剥离，投递动作为随机成败的空 stub。
