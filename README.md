# Campaign Dispatch Demo — NestJS + Temporal + Pulsar

面试用技术架构 demo。从生产服务抽取 **Campaign 派发核心链路**，剥离全部业务逻辑（投递动作=空 stub），
聚焦展示：**Temporal 工作流编排、Pulsar 消息派发、基于 CAS 的状态机幂等、epoch 防陈旧消息栅栏、暂停/恢复 rewind、对账兜底、六边形端口分层、代码规范**。

三个部分：
- `backend/` — 真实可运行的 NestJS 服务（连真实 Temporal / Pulsar / Mongo / Redis）
- `frontend/` — Vite + React 运行时控制台（列表、派发/暂停/恢复、SSE 实时事件流）
- `demo/` — 静态 keynote 风格演示文稿，打开 `demo/index.html`，用 ←/→ 翻页

---

## 架构

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
   + Pause/Resume：pause=Redis flag；resume=clear flag + epoch++ + IN_PROGRESS 回退 PENDING
```

Delivery 状态机：`PENDING → IN_PROGRESS → SENDING → SUCCESS / FAILED`（全部走原子 CAS）。

---

## Repository layout

| Part | Path | What it is |
|---|---|---|
| Backend | `backend/` | NestJS + Temporal + Pulsar campaign dispatch engine |
| Console | `frontend/` | Vite + React runtime console (list, dispatch/pause/resume, live SSE event stream) |
| Demo | `demo/` | Static keynote-style presentation — open `demo/index.html`, navigate with ←/→ |

## Quick start

```bash
docker compose up -d
cd backend && npm i && npm run build && node dist/main.js
# new terminal
cd frontend && npm i && npm run dev    # console on http://localhost:5173
```

## 运行后端

前置：Docker、Node 20+。

```bash
# 1. 起依赖全家桶（Temporal + Temporal UI + Pulsar + Mongo + Redis）
docker compose up -d

# 2. 起后端（API + 两个 Temporal worker + Pulsar consumer + cron）
cd backend
npm install
cp .env.example .env
npm run build && npm start          # 或 npm run start:dev
```

端口：
- API: http://localhost:3000
- Temporal UI: http://localhost:8080 （可看到父/子 workflow）
- Pulsar admin: http://localhost:8081

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

---

## 运行前端控制台

```bash
cd frontend && npm i && npm run dev    # http://localhost:5173
```

实时 SSE 事件流、暂停/恢复操作、派发进度一览。

---

## 设计亮点（面试讲点）

| 亮点 | 说明 |
|------|------|
| **Temporal 父子 workflow** | 父跑 targeting 后 `startChild` 派发；`ParentClosePolicy.ABANDON` 让父完成后子继续；proxyActivities 显式配置 retry/timeout |
| **CAS 幂等** | 所有状态转换 = 单条 `updateOne(filter:{status:from})` 看 `modifiedCount`，无读后写；Temporal 重试与并发消费天然去重 |
| **Mark-first 派发** | 先 CAS 占位再发消息，only-on-win；崩溃留下的孤儿行由 rewind/对账收尾 |
| **Pulsar Shared 订阅** | 水平扩展消费；不 ack 即重投 = at-least-once，配合下游 CAS = effectively-once；绝不静默丢消息 |
| **epoch 栅栏** | 暂停/恢复时 epoch++，在途旧消息 `epoch<current` 被消费端丢弃，防止陈旧消息污染状态 |
| **暂停/恢复 rewind** | resume 把被栅栏丢弃的 IN_PROGRESS 行回退 PENDING，新一轮重投——不重复、不丢行 |
| **对账兜底** | 完成判定交独立 cron（dispatcher 可能 ABANDON/崩溃提前退出）→ 最终一致性 |
| **六边形端口分层** | activity 依赖 `ICampaignDeliveryDispatchPort` 接口而非具体实现；interface-first + token 注入 |

> 注：业务逻辑（发奖/预算/SMS/Email/Mail 三通道/recurring 定时）已全部剥离，投递动作为随机成败的空 stub。
