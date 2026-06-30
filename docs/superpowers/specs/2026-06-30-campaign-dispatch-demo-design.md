# Campaign 派发核心流程 Demo — 设计文档

> 目的：从 `FPMS-NT-User-Engagement` 抽取 **Campaign + Temporal + Pulsar 派发** 的核心技术架构，
> 剥离全部业务逻辑，用于面试时讲解技术架构、核心设计流程与代码规范。
> 分两部分：**后端（真实可运行）** 与 **前端（纯静态讲解页，不接 API）**。

## 1. 范围

### 保留
- 主派发链路：`executeCampaignWorkflow` → `campaignDispatcherWorkflow` → `dispatchPlayers` activity → Pulsar → `CampaignDeliveryConsumer`
- CAS 幂等：逐行原子状态流转 `PENDING → IN_PROGRESS → SENDING → SUCCESS/FAILED`
- epoch 防陈旧消息栅栏
- 暂停/恢复：Redis pause flag + epoch++（rewind）
- Reconciliation 对账 cron：收尾孤儿投递、标记 campaign `COMPLETED`
- 原项目代码规范：interface-first 端口注入、英文「为什么」注释、显式 retry/timeout

### 剥离（不保留）
- grant 发奖、budget 预算
- SMS/Email/Mail 三通道 → 投递动作换成**空 stub**（sleep + 随机成功/失败 + log）
- Recurring 定时 + 子活动（createChildCampaign）→ 去掉，简化父 workflow

## 2. 整体结构

```
nestjs-temporal-pulsar-demo/
├── docker-compose.yml          # Temporal + Temporal UI + Pulsar + Mongo + Redis
├── backend/                    # NestJS 可运行后端
│   └── src/...
└── frontend/
    └── index.html              # 纯静态单页交互架构图（vanilla HTML/CSS/JS）
```

## 3. 派发链路

```
POST /campaigns/:id/dispatch
   └─→ Temporal: executeCampaignWorkflow (父)
          ├─ targeting activity            # 生成 N 条 PENDING delivery（空数据）
          └─ startChild → campaignDispatcherWorkflow   # ParentClosePolicy.ABANDON
                 └─ dispatchPlayers activity
                      ├─ campaign CAS: PENDING→IN_PROGRESS（原子、幂等）
                      ├─ while: 分页拉取 PENDING delivery
                      │    ├─ check pause flag (Redis) → break
                      │    ├─ 逐行 CAS PENDING→IN_PROGRESS（mark-first）
                      │    └─ 命中则 Pulsar.send({deliveryId, campaignId, epoch})
                      └─ heartbeat(processed)
                                    │
                                    ▼  Pulsar topic（Shared 订阅）
                      CampaignDeliveryConsumer
                      ├─ 重读 delivery（必须 IN_PROGRESS，否则 ack-skip）
                      ├─ epoch 栅栏：msg.epoch < currentEpoch → ack-skip
                      ├─ CAS IN_PROGRESS→SENDING（输者 ack-skip）
                      ├─ deliverStub()           # 空投递：sleep + 随机成功/失败
                      └─ markTerminal SUCCESS/FAILED
   + Reconciliation cron：扫描 IN_PROGRESS campaign，孤儿投递收尾 → campaign COMPLETED
   + Pause/Resume API：POST /campaigns/:id/pause  → redis flag=1
                       POST /campaigns/:id/resume → flag=0 + epoch++ (rewind 重新派发)
```

## 4. 后端模块设计（六边形 / 端口分层）

| 模块 | 职责 |
|------|------|
| `modules/campaign` | Campaign CRUD、dispatch/pause/resume 入口（Controller/Service） |
| `modules/campaign-temporal-scheduler` | `executeCampaignWorkflow` + `targeting` activity |
| `modules/campaign-delivery-temporal` | `campaignDispatcherWorkflow` + `dispatchPlayers` activity |
| `modules/campaign-delivery` | delivery service + 状态机方法（`markInProgressIfPending` / `markSendingIfInProgress` / `markTerminal`） |
| `libs/pulsar` | `CampaignDeliveryProducer` + `CampaignDeliveryConsumer`（保留 ack/nack 重试语义） |
| `libs/temporal` | Temporal 连接、worker 注册、workflow proxy |
| `libs/redis` | pause flag + epoch 读写 |
| `orchestration/reconciliation` | 对账 cron |
| `interfaces/*` | 端口接口（`Xxx_Activity_Interface` 命名、英文 WHY 注释） |

### 状态机（delivery）
```
PENDING ──CAS──> IN_PROGRESS ──CAS──> SENDING ──> SUCCESS
                                              └──> FAILED
（暂停/rewind：epoch++ 使在途消息陈旧；未发的行保持 PENDING 等重派）
```

### 代码规范亮点（面试讲点）
- interface-first 端口注入：`@Inject(TOKEN)` + `ICampaignDeliveryDispatchPort`
- activity 幂等：所有状态转换走原子 CAS（`updateOne(filter:{status:from}, set:{status:to})` 看 `modifiedCount`）
- 注释只写「为什么」，不写「做什么」
- retry / timeout / concurrency 全部显式配置

## 5. 前端讲解页

- 单 HTML 文件，vanilla HTML/CSS/JS，零构建，双击即开，**不接任何 API**
- 顶部可交互派发链路图（HTML/SVG 节点）：点击节点 → 展开「做什么 + 为什么这样设计 + 后端代码片段（静态嵌入、语法高亮）」
- 数据状态机小图：`PENDING→IN_PROGRESS→SENDING→SUCCESS/FAILED` 的 CAS 流转动画（纯 CSS）
- 锚点/标签：架构总览 / Temporal 编排 / Pulsar + 幂等&epoch
- 所有「流转」用 CSS 动画演示，不调后端

## 6. 验收标准

### 后端
- `docker-compose up` 起全家桶，`npm run start:dev` 起后端 worker + API
- `POST /campaigns`（建活动 + N 条空 delivery）→ `POST /campaigns/:id/dispatch`
- Temporal UI 能看到 `executeCampaignWorkflow` 与子 `campaignDispatcherWorkflow`
- delivery 状态最终全部到 SUCCESS/FAILED 终态
- `pause` 后在途消息被 epoch 栅栏丢弃；`resume`（epoch++）后剩余 PENDING 重新派发完成
- 对账 cron 把 campaign 标记 COMPLETED

### 前端
- 浏览器直接打开 `frontend/index.html` 即可浏览
- 每个链路节点可点击展开说明 + 代码片段
- 不产生任何网络请求
