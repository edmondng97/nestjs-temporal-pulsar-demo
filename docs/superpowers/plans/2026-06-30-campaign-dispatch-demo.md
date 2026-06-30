# Campaign 派发核心流程 Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 抽取 Campaign + Temporal + Pulsar 派发核心架构成一个可运行的后端 + 纯静态讲解前端，用于面试展示技术架构与代码规范。

**Architecture:** NestJS 单进程同时承载 HTTP API、Temporal worker、Pulsar consumer。派发链路：HTTP 触发 → Temporal `executeCampaignWorkflow`（父）→ `targeting` activity 生成空 delivery → `startChild` 到 `campaignDispatcherWorkflow` → `dispatchPlayers` activity（分页 + 逐行 CAS + 发 Pulsar）→ `CampaignDeliveryConsumer`（重读 + epoch 栅栏 + CAS + 空投递 + 终态）。配套 Redis pause/epoch、reconciliation cron。前端为零构建单 HTML 交互讲解页。

**Tech Stack:** NestJS 10, TypeScript 5, @temporalio/{client,worker,workflow,activity,common}, pulsar-client, @nestjs/mongoose + mongoose, ioredis, @nestjs/schedule。基础设施用 docker-compose（Temporal + Temporal UI + Pulsar + Mongo + Redis）。测试用 jest + mongodb-memory-server。

## Global Constraints

- 代码注释一律英文，且只解释「为什么」不解释「做什么」
- 所有 delivery / campaign 状态转换必须走原子 CAS（`updateOne(filter:{status:from}) → modifiedCount`），不得读后写
- 端口注入用 interface + `@Inject(TOKEN)`，接口命名 `Xxx_Interface`
- 业务逻辑全部剥离：投递动作为空 stub（sleep + 随机成功/失败 + log）
- 不保留 grant/budget/三通道/recurring
- 前端纯静态，零网络请求，单 HTML 文件 vanilla JS
- 后端目录根为 `backend/`，前端为 `frontend/`
- 未经用户允许不得 `git commit`（用户全局规则）；本计划中所有「Commit」步骤需先获用户同意，或由执行者按用户当时指示处理

---

## File Structure

```
docker-compose.yml
backend/
  package.json  tsconfig.json  nest-cli.json  .env.example  jest.config.ts
  src/
    main.ts                                  # bootstrap: API + temporal worker + pulsar consumer
    app.module.ts
    constants.ts                             # statuses, DI tokens, topic/queue names
    schemas/campaign.schema.ts
    schemas/campaign-delivery.schema.ts
    interfaces/
      campaign-delivery.port.ts              # ICampaignDeliveryDispatchPort + token
      activities.interface.ts                # *_Activity_Interface 签名
    libs/
      temporal/temporal.constants.ts         # workflow types, taskqueues, id builders
      temporal/workflow.ts                   # re-export @temporalio/workflow (sandbox-safe import seam)
      temporal/temporal-client.service.ts    # WorkflowClient provider
      temporal/temporal-worker.bootstrap.ts  # Worker.create + run
      pulsar/pulsar.constants.ts             # topic, subscription, client token, message type
      pulsar/pulsar-client.provider.ts       # Pulsar Client provider
      pulsar/campaign-delivery.producer.ts
      pulsar/campaign-delivery.consumer.ts
      redis/redis.service.ts                 # pause flag + epoch get/incr
    modules/
      campaign/campaign.service.ts
      campaign/campaign.controller.ts
      campaign/campaign.module.ts
      campaign-delivery/campaign-delivery.service.ts   # CAS state machine
      campaign-delivery/campaign-delivery.module.ts
      campaign-temporal-scheduler/workflows/execute-campaign.workflow.ts
      campaign-temporal-scheduler/activities/targeting.activity.ts
      campaign-delivery-temporal/workflows/campaign-dispatcher.workflow.ts
      campaign-delivery-temporal/activities/dispatch-players.activity.ts
    orchestration/reconciliation/reconciliation.service.ts   # @Cron
  test/
    campaign-delivery.service.spec.ts
    redis.service.spec.ts
    reconciliation.service.spec.ts
frontend/
  index.html
  styles.css
  app.js
  snippets.js                                # 静态嵌入的后端代码片段（讲解用）
README.md
```

---

## Task 1: 基础设施 docker-compose + 后端脚手架

**Files:**
- Create: `docker-compose.yml`
- Create: `backend/package.json`, `backend/tsconfig.json`, `backend/nest-cli.json`, `backend/.env.example`, `backend/jest.config.ts`
- Create: `backend/src/constants.ts`

**Interfaces:**
- Produces: 常量 — 状态枚举、DI token、topic/queue 名，供后续所有任务消费。

- [ ] **Step 1: 写 docker-compose.yml**

```yaml
services:
  postgresql:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: temporal
      POSTGRES_USER: temporal
    ports: ["5432:5432"]
  temporal:
    image: temporalio/auto-setup:1.25.0
    depends_on: [postgresql]
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: temporal
      POSTGRES_PWD: temporal
      POSTGRES_SEEDS: postgresql
    ports: ["7233:7233"]
  temporal-ui:
    image: temporalio/ui:2.31.0
    depends_on: [temporal]
    environment:
      TEMPORAL_ADDRESS: temporal:7233
    ports: ["8080:8080"]
  pulsar:
    image: apachepulsar/pulsar:3.2.0
    command: bin/pulsar standalone
    ports: ["6650:6650", "8081:8080"]
  mongo:
    image: mongo:7
    ports: ["27017:27017"]
  redis:
    image: redis:7
    ports: ["6379:6379"]
```

- [ ] **Step 2: 写 backend/package.json**

```json
{
  "name": "campaign-dispatch-demo-backend",
  "version": "1.0.0",
  "scripts": {
    "build": "nest build",
    "start:dev": "nest start --watch",
    "start": "node dist/main.js",
    "test": "jest --config ./jest.config.ts"
  },
  "dependencies": {
    "@nestjs/common": "^10.4.0",
    "@nestjs/core": "^10.4.0",
    "@nestjs/mongoose": "^10.1.0",
    "@nestjs/platform-express": "^10.4.0",
    "@nestjs/schedule": "^4.1.2",
    "@temporalio/activity": "^1.11.0",
    "@temporalio/client": "^1.11.0",
    "@temporalio/common": "^1.11.0",
    "@temporalio/worker": "^1.11.0",
    "@temporalio/workflow": "^1.11.0",
    "ioredis": "^5.4.1",
    "mongoose": "^8.7.0",
    "pulsar-client": "^1.13.0",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  },
  "devDependencies": {
    "@nestjs/cli": "^10.4.5",
    "@nestjs/testing": "^10.4.0",
    "@types/jest": "^29.5.13",
    "@types/node": "^22.7.0",
    "jest": "^29.7.0",
    "mongodb-memory-server": "^10.1.2",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 3: 写 tsconfig.json / nest-cli.json / jest.config.ts / .env.example**

`backend/tsconfig.json`:
```json
{
  "compilerOptions": {
    "module": "commonjs",
    "target": "ES2021",
    "moduleResolution": "node",
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true,
    "esModuleInterop": true,
    "declaration": false,
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": "./",
    "strict": true,
    "strictPropertyInitialization": false,
    "skipLibCheck": true
  }
}
```

`backend/nest-cli.json`:
```json
{ "collection": "@nestjs/schematics", "sourceRoot": "src", "compilerOptions": { "deleteOutDir": true } }
```

`backend/jest.config.ts`:
```ts
import type { Config } from 'jest';
const config: Config = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  testMatch: ['<rootDir>/test/**/*.spec.ts'],
  testTimeout: 30000,
};
export default config;
```

`backend/.env.example`:
```
MONGO_URI=mongodb://localhost:27017/campaign_demo
REDIS_URL=redis://localhost:6379
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
PULSAR_URL=pulsar://localhost:6650
```

- [ ] **Step 4: 写 src/constants.ts**

```ts
// Single source of truth for status enums, DI tokens, and infra names.
// Centralised so workflows (sandbox), activities, and Nest providers agree.

export const CAMPAIGN_STATUS = {
  PENDING: 'PENDING',
  IN_PROGRESS: 'IN_PROGRESS',
  COMPLETED: 'COMPLETED',
} as const;

export const DELIVERY_STATUS = {
  PENDING: 'PENDING',
  IN_PROGRESS: 'IN_PROGRESS',
  SENDING: 'SENDING',
  SUCCESS: 'SUCCESS',
  FAILED: 'FAILED',
} as const;

export const TOKENS = {
  CAMPAIGN_DELIVERY_DISPATCH_PORT: 'CAMPAIGN_DELIVERY_DISPATCH_PORT',
  PULSAR_CLIENT: 'PULSAR_CLIENT',
  TEMPORAL_CLIENT: 'TEMPORAL_CLIENT',
} as const;
```

- [ ] **Step 5: 验证依赖可装**

Run: `cd backend && npm install`
Expected: 安装成功，无 peer error 阻断（pulsar-client 需本机有预编译二进制；macOS arm64 由 npm 自动拉取）。

- [ ] **Step 6: Commit**（需用户同意）

```bash
git add docker-compose.yml backend/package.json backend/tsconfig.json backend/nest-cli.json backend/jest.config.ts backend/.env.example backend/src/constants.ts
git commit -m "chore: scaffold backend + docker-compose infra"
```

---

## Task 2: Mongoose schemas

**Files:**
- Create: `backend/src/schemas/campaign.schema.ts`, `backend/src/schemas/campaign-delivery.schema.ts`

**Interfaces:**
- Produces: `Campaign`/`CampaignDelivery` Mongoose 文档类型与 schema，供 service/repository 消费。`CampaignDelivery` 含 `campaignId`, `status`, `completedAt?`, `errorMessage?`；`Campaign` 含 `status`, `dispatchEpoch`, `lastExecutionDate?`。

- [ ] **Step 1: 写 campaign.schema.ts**

```ts
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import { CAMPAIGN_STATUS } from '../constants';

export type CampaignDocument = HydratedDocument<Campaign>;

@Schema({ collection: 'campaigns', timestamps: true })
export class Campaign {
  @Prop({ required: true, default: CAMPAIGN_STATUS.PENDING })
  status: string;

  // Monotonic dispatch generation. Bumped on resume so in-flight (pre-pause)
  // messages become stale and are fenced out at the consumer.
  @Prop({ required: true, default: 0 })
  dispatchEpoch: number;

  @Prop()
  lastExecutionDate?: Date;
}

export const CampaignSchema = SchemaFactory.createForClass(Campaign);
```

- [ ] **Step 2: 写 campaign-delivery.schema.ts**

```ts
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Types } from 'mongoose';
import { DELIVERY_STATUS } from '../constants';

export type CampaignDeliveryDocument = HydratedDocument<CampaignDelivery>;

@Schema({ collection: 'campaign_deliveries', timestamps: true })
export class CampaignDelivery {
  @Prop({ type: Types.ObjectId, required: true, index: true })
  campaignId: Types.ObjectId;

  @Prop({ required: true, default: DELIVERY_STATUS.PENDING, index: true })
  status: string;

  @Prop()
  completedAt?: Date;

  @Prop()
  errorMessage?: string;
}

export const CampaignDeliverySchema = SchemaFactory.createForClass(CampaignDelivery);
```

- [ ] **Step 3: Commit**（需用户同意）

```bash
git add backend/src/schemas
git commit -m "feat: add campaign + delivery mongoose schemas"
```

---

## Task 3: CampaignDeliveryService — CAS 状态机（TDD）

**Files:**
- Create: `backend/src/modules/campaign-delivery/campaign-delivery.service.ts`
- Create: `backend/src/modules/campaign-delivery/campaign-delivery.module.ts`
- Create: `backend/src/interfaces/campaign-delivery.port.ts`
- Test: `backend/test/campaign-delivery.service.spec.ts`

**Interfaces:**
- Produces:
  - `markInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }>` — CAS PENDING→IN_PROGRESS
  - `markSendingIfInProgress(id: Types.ObjectId): Promise<{ modifiedCount: number }>` — CAS IN_PROGRESS→SENDING
  - `markTerminal(input: { deliveryId: Types.ObjectId; status: string; completedAt: Date; errorMessage?: string }): Promise<void>`
  - `findOne(filter): Promise<CampaignDeliveryDocument | null>`
  - `findPendingPage(campaignId: Types.ObjectId, limit: number): Promise<CampaignDeliveryDocument[]>` — 拉 PENDING 页
  - `createMany(campaignId, count): Promise<void>` — targeting 用，建 count 条 PENDING
  - `ICampaignDeliveryDispatchPort` 端口暴露 `findPendingPage` + `markInProgressIfPending`

- [ ] **Step 1: 写失败测试 `test/campaign-delivery.service.spec.ts`**

```ts
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose, { Connection, Types } from 'mongoose';
import { CampaignDeliverySchema } from '../src/schemas/campaign-delivery.schema';
import { CampaignDeliveryService } from '../src/modules/campaign-delivery/campaign-delivery.service';
import { DELIVERY_STATUS } from '../src/constants';

describe('CampaignDeliveryService CAS', () => {
  let mongod: MongoMemoryServer;
  let conn: Connection;
  let service: CampaignDeliveryService;

  beforeAll(async () => {
    mongod = await MongoMemoryServer.create();
    conn = (await mongoose.createConnection(mongod.getUri()).asPromise());
    const model = conn.model('CampaignDelivery', CampaignDeliverySchema);
    service = new CampaignDeliveryService(model as any);
  });
  afterAll(async () => { await conn.close(); await mongod.stop(); });

  it('markInProgressIfPending wins once, loses on re-run', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    const first = await service.markInProgressIfPending(d._id);
    const second = await service.markInProgressIfPending(d._id);
    expect(first.modifiedCount).toBe(1);
    expect(second.modifiedCount).toBe(0);
  });

  it('markSendingIfInProgress only transitions from IN_PROGRESS', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    expect((await service.markSendingIfInProgress(d._id)).modifiedCount).toBe(0); // still PENDING
    await service.markInProgressIfPending(d._id);
    expect((await service.markSendingIfInProgress(d._id)).modifiedCount).toBe(1);
  });

  it('markTerminal writes status + completedAt', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    await service.markTerminal({ deliveryId: d._id, status: DELIVERY_STATUS.SUCCESS, completedAt: new Date() });
    const after = await service.findOne({ _id: d._id });
    expect(after!.status).toBe(DELIVERY_STATUS.SUCCESS);
    expect(after!.completedAt).toBeDefined();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && npx jest campaign-delivery.service`
Expected: FAIL，`CampaignDeliveryService` 未定义 / 模块找不到。

- [ ] **Step 3: 写端口 `src/interfaces/campaign-delivery.port.ts`**

```ts
import { Types } from 'mongoose';
import { CampaignDeliveryDocument } from '../schemas/campaign-delivery.schema';

// Port consumed by the dispatch activity. Keeps the activity decoupled from the
// concrete Mongoose service so it depends on behaviour, not implementation.
export interface ICampaignDeliveryDispatchPort {
  findPendingPage(campaignId: Types.ObjectId, limit: number): Promise<CampaignDeliveryDocument[]>;
  markInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }>;
}
```

- [ ] **Step 4: 写 `campaign-delivery.service.ts`**

```ts
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types, FilterQuery } from 'mongoose';
import { CampaignDelivery, CampaignDeliveryDocument } from '../../schemas/campaign-delivery.schema';
import { DELIVERY_STATUS } from '../../constants';
import { ICampaignDeliveryDispatchPort } from '../../interfaces/campaign-delivery.port';

@Injectable()
export class CampaignDeliveryService implements ICampaignDeliveryDispatchPort {
  constructor(
    @InjectModel(CampaignDelivery.name) private readonly model: Model<CampaignDeliveryDocument>,
  ) {}

  async createMany(campaignId: Types.ObjectId, count: number): Promise<void> {
    const docs = Array.from({ length: count }, () => ({
      campaignId,
      status: DELIVERY_STATUS.PENDING,
    }));
    await this.model.insertMany(docs);
  }

  findOne(filter: FilterQuery<CampaignDeliveryDocument>): Promise<CampaignDeliveryDocument | null> {
    return this.model.findOne(filter).exec();
  }

  findPendingPage(campaignId: Types.ObjectId, limit: number): Promise<CampaignDeliveryDocument[]> {
    return this.model
      .find({ campaignId, status: DELIVERY_STATUS.PENDING })
      .limit(limit)
      .exec();
  }

  // CAS PENDING -> IN_PROGRESS. modifiedCount=1 means this caller claimed the row.
  async markInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: DELIVERY_STATUS.PENDING },
      { $set: { status: DELIVERY_STATUS.IN_PROGRESS } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async markSendingIfInProgress(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: DELIVERY_STATUS.IN_PROGRESS },
      { $set: { status: DELIVERY_STATUS.SENDING } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async markTerminal(input: {
    deliveryId: Types.ObjectId;
    status: string;
    completedAt: Date;
    errorMessage?: string;
  }): Promise<void> {
    await this.model.updateOne(
      { _id: input.deliveryId },
      {
        $set: {
          status: input.status,
          completedAt: input.completedAt,
          ...(input.errorMessage ? { errorMessage: input.errorMessage } : {}),
        },
      },
    );
  }

  async countByStatus(campaignId: Types.ObjectId, status: string): Promise<number> {
    return this.model.countDocuments({ campaignId, status });
  }
}
```

- [ ] **Step 5: 写 `campaign-delivery.module.ts`**

```ts
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { CampaignDelivery, CampaignDeliverySchema } from '../../schemas/campaign-delivery.schema';
import { CampaignDeliveryService } from './campaign-delivery.service';
import { TOKENS } from '../../constants';

@Module({
  imports: [MongooseModule.forFeature([{ name: CampaignDelivery.name, schema: CampaignDeliverySchema }])],
  providers: [
    CampaignDeliveryService,
    { provide: TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT, useExisting: CampaignDeliveryService },
  ],
  exports: [CampaignDeliveryService, TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT],
})
export class CampaignDeliveryModule {}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `cd backend && npx jest campaign-delivery.service`
Expected: PASS (3 个用例)。

- [ ] **Step 7: Commit**（需用户同意）

```bash
git add backend/src/modules/campaign-delivery backend/src/interfaces/campaign-delivery.port.ts backend/test/campaign-delivery.service.spec.ts
git commit -m "feat: campaign delivery CAS state machine with tests"
```

---

## Task 4: RedisService — pause flag + epoch（TDD）

**Files:**
- Create: `backend/src/libs/redis/redis.service.ts`
- Test: `backend/test/redis.service.spec.ts`

**Interfaces:**
- Produces:
  - `isPaused(campaignId: string): Promise<boolean>`
  - `setPaused(campaignId: string, paused: boolean): Promise<void>`
  - 注：epoch 真值源在 Mongo `campaign.dispatchEpoch`（在 Task 5 的 CampaignService 读写）。Redis 仅存 pause flag。

- [ ] **Step 1: 写失败测试 `test/redis.service.spec.ts`**

```ts
import { RedisService } from '../src/libs/redis/redis.service';

// Uses ioredis-mock to avoid a live redis in unit tests.
jest.mock('ioredis', () => require('ioredis-mock'));

describe('RedisService pause flag', () => {
  let svc: RedisService;
  beforeAll(() => { svc = new RedisService('redis://localhost:6379'); });

  it('defaults to not paused', async () => {
    expect(await svc.isPaused('c1')).toBe(false);
  });
  it('reflects setPaused', async () => {
    await svc.setPaused('c1', true);
    expect(await svc.isPaused('c1')).toBe(true);
    await svc.setPaused('c1', false);
    expect(await svc.isPaused('c1')).toBe(false);
  });
});
```

注：在 package.json devDependencies 增加 `"ioredis-mock": "^8.9.0"`，先 `npm i -D ioredis-mock`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && npx jest redis.service`
Expected: FAIL，`RedisService` 未定义。

- [ ] **Step 3: 写 `redis.service.ts`**

```ts
import { Injectable } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService {
  private readonly client: Redis;

  constructor(url = process.env.REDIS_URL ?? 'redis://localhost:6379') {
    this.client = new Redis(url);
  }

  private pauseKey(campaignId: string): string {
    return `campaign:pause:${campaignId}`;
  }

  async isPaused(campaignId: string): Promise<boolean> {
    return (await this.client.get(this.pauseKey(campaignId))) === '1';
  }

  async setPaused(campaignId: string, paused: boolean): Promise<void> {
    if (paused) await this.client.set(this.pauseKey(campaignId), '1');
    else await this.client.del(this.pauseKey(campaignId));
  }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && npx jest redis.service`
Expected: PASS。

- [ ] **Step 5: Commit**（需用户同意）

```bash
git add backend/src/libs/redis backend/test/redis.service.spec.ts backend/package.json
git commit -m "feat: redis pause flag service with tests"
```

---

## Task 5: CampaignService + Controller（dispatch / pause / resume）

**Files:**
- Create: `backend/src/modules/campaign/campaign.service.ts`
- Create: `backend/src/modules/campaign/campaign.controller.ts`
- Create: `backend/src/modules/campaign/campaign.module.ts`
- Create: `backend/src/libs/temporal/temporal.constants.ts`（本任务需要 workflow type/taskqueue/id builder；完整 worker 在 Task 7-8）
- Create: `backend/src/libs/temporal/temporal-client.service.ts`

**Interfaces:**
- Consumes: `RedisService.setPaused`, `CampaignDeliveryService`（统计）, `TEMPORAL_CLIENT`(WorkflowClient)
- Produces:
  - `CampaignService.create(): Promise<{ id: string }>`
  - `CampaignService.claimInProgressIfPending(id): Promise<{ modifiedCount: number }>`（CAS PENDING→IN_PROGRESS）
  - `CampaignService.findById(id)`, `CampaignService.getDispatchEpoch(id): Promise<number>`, `CampaignService.bumpEpoch(id): Promise<number>`, `CampaignService.markCompleted(id)`
  - HTTP：`POST /campaigns`、`POST /campaigns/:id/dispatch`、`POST /campaigns/:id/pause`、`POST /campaigns/:id/resume`、`GET /campaigns/:id`
  - 常量：`EXECUTE_CAMPAIGN_WORKFLOW_TYPE`, `CAMPAIGN_DISPATCHER_WORKFLOW_TYPE`, `SCHEDULER_TASK_QUEUE`, `DISPATCHER_TASK_QUEUE`, `buildExecuteWorkflowId(id)`, `buildDispatcherWorkflowId(id)`

- [ ] **Step 1: 写 `libs/temporal/temporal.constants.ts`**

```ts
export const SCHEDULER_TASK_QUEUE = 'campaign-scheduler';
export const DISPATCHER_TASK_QUEUE = 'campaign-dispatcher';

export const EXECUTE_CAMPAIGN_WORKFLOW_TYPE = 'executeCampaignWorkflow';
export const CAMPAIGN_DISPATCHER_WORKFLOW_TYPE = 'campaignDispatcherWorkflow';

export const buildExecuteWorkflowId = (campaignId: string) => `execute-${campaignId}`;
export const buildDispatcherWorkflowId = (campaignId: string) => `dispatch-${campaignId}`;
```

- [ ] **Step 2: 写 `libs/temporal/temporal-client.service.ts`**

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { Connection, WorkflowClient } from '@temporalio/client';

// Thin wrapper exposing a connected WorkflowClient as a Nest provider.
@Injectable()
export class TemporalClientService implements OnModuleInit {
  private _client: WorkflowClient;

  async onModuleInit(): Promise<void> {
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
    });
    this._client = new WorkflowClient({
      connection,
      namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    });
  }

  get client(): WorkflowClient {
    return this._client;
  }
}
```

- [ ] **Step 3: 写 `campaign.service.ts`**

```ts
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import { Campaign, CampaignDocument } from '../../schemas/campaign.schema';
import { CAMPAIGN_STATUS } from '../../constants';

@Injectable()
export class CampaignService {
  constructor(
    @InjectModel(Campaign.name) private readonly model: Model<CampaignDocument>,
  ) {}

  async create(): Promise<{ id: string }> {
    const doc = await this.model.create({ status: CAMPAIGN_STATUS.PENDING, dispatchEpoch: 0 });
    return { id: doc._id.toString() };
  }

  findById(id: Types.ObjectId): Promise<CampaignDocument | null> {
    return this.model.findById(id).exec();
  }

  // CAS PENDING -> IN_PROGRESS. Idempotent dispatch claim.
  async claimInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: CAMPAIGN_STATUS.PENDING },
      { $set: { status: CAMPAIGN_STATUS.IN_PROGRESS, lastExecutionDate: new Date() } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async getDispatchEpoch(id: Types.ObjectId): Promise<number> {
    const doc = await this.model.findById(id, { dispatchEpoch: 1 }).exec();
    return doc?.dispatchEpoch ?? 0;
  }

  // Atomic increment so resume fences in-flight messages without a read-then-write race.
  async bumpEpoch(id: Types.ObjectId): Promise<number> {
    const doc = await this.model.findByIdAndUpdate(
      id,
      { $inc: { dispatchEpoch: 1 } },
      { new: true },
    ).exec();
    return doc?.dispatchEpoch ?? 0;
  }

  async markCompleted(id: Types.ObjectId): Promise<void> {
    await this.model.updateOne(
      { _id: id, status: CAMPAIGN_STATUS.IN_PROGRESS },
      { $set: { status: CAMPAIGN_STATUS.COMPLETED } },
    );
  }

  async listInProgress(): Promise<CampaignDocument[]> {
    return this.model.find({ status: CAMPAIGN_STATUS.IN_PROGRESS }).exec();
  }
}
```

- [ ] **Step 4: 写 `campaign.controller.ts`**

```ts
import { Controller, Get, Param, Post } from '@nestjs/common';
import { Types } from 'mongoose';
import { CampaignService } from './campaign.service';
import { CampaignDeliveryService } from '../campaign-delivery/campaign-delivery.service';
import { RedisService } from '../../libs/redis/redis.service';
import { TemporalClientService } from '../../libs/temporal/temporal-client.service';
import {
  EXECUTE_CAMPAIGN_WORKFLOW_TYPE,
  SCHEDULER_TASK_QUEUE,
  DISPATCHER_TASK_QUEUE,
  buildExecuteWorkflowId,
} from '../../libs/temporal/temporal.constants';
import { DELIVERY_STATUS } from '../../constants';

@Controller('campaigns')
export class CampaignController {
  constructor(
    private readonly campaignService: CampaignService,
    private readonly deliveryService: CampaignDeliveryService,
    private readonly redis: RedisService,
    private readonly temporal: TemporalClientService,
  ) {}

  @Post()
  create() {
    return this.campaignService.create();
  }

  // Kick off the parent workflow. Dispatcher task queue is passed in so the
  // child runs on its own worker pool — mirrors the source service topology.
  @Post(':id/dispatch')
  async dispatch(@Param('id') id: string) {
    await this.temporal.client.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, {
      taskQueue: SCHEDULER_TASK_QUEUE,
      workflowId: buildExecuteWorkflowId(id),
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE }],
    });
    return { started: true };
  }

  @Post(':id/pause')
  async pause(@Param('id') id: string) {
    await this.redis.setPaused(id, true);
    return { paused: true };
  }

  // Resume = clear pause flag + bump epoch. Old in-flight messages become stale
  // and are fenced at the consumer; un-sent PENDING rows get re-dispatched.
  @Post(':id/resume')
  async resume(@Param('id') id: string) {
    await this.redis.setPaused(id, false);
    const epoch = await this.campaignService.bumpEpoch(new Types.ObjectId(id));
    await this.temporal.client.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, {
      taskQueue: SCHEDULER_TASK_QUEUE,
      workflowId: `${buildExecuteWorkflowId(id)}-resume-${epoch}`,
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE }],
    });
    return { resumed: true, epoch };
  }

  @Get(':id')
  async get(@Param('id') id: string) {
    const oid = new Types.ObjectId(id);
    const campaign = await this.campaignService.findById(oid);
    const counts = {
      PENDING: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.PENDING),
      IN_PROGRESS: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.IN_PROGRESS),
      SENDING: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.SENDING),
      SUCCESS: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.SUCCESS),
      FAILED: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.FAILED),
    };
    return { campaign, counts };
  }
}
```

- [ ] **Step 5: 写 `campaign.module.ts`**

```ts
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Campaign, CampaignSchema } from '../../schemas/campaign.schema';
import { CampaignService } from './campaign.service';
import { CampaignController } from './campaign.controller';
import { CampaignDeliveryModule } from '../campaign-delivery/campaign-delivery.module';
import { RedisService } from '../../libs/redis/redis.service';
import { TemporalClientService } from '../../libs/temporal/temporal-client.service';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: Campaign.name, schema: CampaignSchema }]),
    CampaignDeliveryModule,
  ],
  controllers: [CampaignController],
  providers: [CampaignService, RedisService, TemporalClientService],
  exports: [CampaignService, RedisService, TemporalClientService],
})
export class CampaignModule {}
```

- [ ] **Step 6: 编译确认**

Run: `cd backend && npx tsc --noEmit`
Expected: 通过（workflow 文件尚未引用，无类型错误）。

- [ ] **Step 7: Commit**（需用户同意）

```bash
git add backend/src/modules/campaign backend/src/libs/temporal/temporal.constants.ts backend/src/libs/temporal/temporal-client.service.ts
git commit -m "feat: campaign service + controller (dispatch/pause/resume)"
```

---

## Task 6: Pulsar producer + consumer + client provider

**Files:**
- Create: `backend/src/libs/pulsar/pulsar.constants.ts`
- Create: `backend/src/libs/pulsar/pulsar-client.provider.ts`
- Create: `backend/src/libs/pulsar/campaign-delivery.producer.ts`
- Create: `backend/src/libs/pulsar/campaign-delivery.consumer.ts`
- Create: `backend/src/libs/pulsar/pulsar.module.ts`

**Interfaces:**
- Consumes: `CampaignDeliveryService`, `CampaignService`, `PULSAR_CLIENT`
- Produces:
  - `CampaignDeliveryProducer.send(msg: CampaignDeliveryMessage): Promise<void>`
  - `CampaignDeliveryMessage = { deliveryId: string; campaignId: string; epoch: number }`
  - `CampaignDeliveryConsumer.start(): Promise<void>`（在 main 里手动启动 receive loop）

- [ ] **Step 1: 写 `pulsar.constants.ts`**

```ts
import { TOKENS } from '../../constants';

export const CAMPAIGN_DELIVERY_TOPIC = 'persistent://public/default/campaign-delivery';
export const CAMPAIGN_DELIVERY_SUBSCRIPTION = 'campaign-delivery-sub';
export const CAMPAIGN_DELIVERY_PULSAR_CLIENT = TOKENS.PULSAR_CLIENT;

export interface CampaignDeliveryMessage {
  deliveryId: string;
  campaignId: string;
  epoch: number;
}
```

- [ ] **Step 2: 写 `pulsar-client.provider.ts`**

```ts
import { Provider } from '@nestjs/common';
import { Client } from 'pulsar-client';
import { TOKENS } from '../../constants';

// A single Pulsar Client shared by producer + consumer. Closed on shutdown by Nest.
export const PulsarClientProvider: Provider = {
  provide: TOKENS.PULSAR_CLIENT,
  useFactory: () =>
    new Client({ serviceUrl: process.env.PULSAR_URL ?? 'pulsar://localhost:6650' }),
};
```

- [ ] **Step 3: 写 `campaign-delivery.producer.ts`**

```ts
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Client, Producer } from 'pulsar-client';
import {
  CAMPAIGN_DELIVERY_PULSAR_CLIENT,
  CAMPAIGN_DELIVERY_TOPIC,
  CampaignDeliveryMessage,
} from './pulsar.constants';

@Injectable()
export class CampaignDeliveryProducer implements OnModuleInit {
  private producer: Producer;

  constructor(@Inject(CAMPAIGN_DELIVERY_PULSAR_CLIENT) private readonly client: Client) {}

  async onModuleInit(): Promise<void> {
    this.producer = await this.client.createProducer({ topic: CAMPAIGN_DELIVERY_TOPIC });
  }

  async send(msg: CampaignDeliveryMessage): Promise<void> {
    await this.producer.send({ data: Buffer.from(JSON.stringify(msg)) });
  }
}
```

- [ ] **Step 4: 写 `campaign-delivery.consumer.ts`**

```ts
import { Inject, Injectable } from '@nestjs/common';
import { Client, Consumer } from 'pulsar-client';
import { Types } from 'mongoose';
import {
  CAMPAIGN_DELIVERY_PULSAR_CLIENT,
  CAMPAIGN_DELIVERY_TOPIC,
  CAMPAIGN_DELIVERY_SUBSCRIPTION,
  CampaignDeliveryMessage,
} from './pulsar.constants';
import { CampaignDeliveryService } from '../../modules/campaign-delivery/campaign-delivery.service';
import { CampaignService } from '../../modules/campaign/campaign.service';
import { DELIVERY_STATUS } from '../../constants';

@Injectable()
export class CampaignDeliveryConsumer {
  private consumer: Consumer;

  constructor(
    @Inject(CAMPAIGN_DELIVERY_PULSAR_CLIENT) private readonly client: Client,
    private readonly deliveryService: CampaignDeliveryService,
    private readonly campaignService: CampaignService,
  ) {}

  // Shared subscription → many consumers can drain the same backlog in parallel.
  async start(): Promise<void> {
    this.consumer = await this.client.subscribe({
      topic: CAMPAIGN_DELIVERY_TOPIC,
      subscription: CAMPAIGN_DELIVERY_SUBSCRIPTION,
      subscriptionType: 'Shared',
      receiverQueueSize: 100,
      ackTimeoutMs: 5 * 60 * 1000,
    });
    void this.receiveLoop();
  }

  private async receiveLoop(): Promise<void> {
    // Run forever; each iteration handles one message then acks/nacks.
    for (;;) {
      const msg = await this.consumer.receive();
      const data = JSON.parse(msg.getData().toString()) as CampaignDeliveryMessage;
      try {
        await this.handle(data);
        await this.consumer.acknowledge(msg);
      } catch (err) {
        // Do NOT ack on unknown error → Pulsar redelivers after ackTimeout.
        // A blanket ack here would silently drop deliveries forever.
        this.consumer.negativeAcknowledge(msg);
      }
    }
  }

  private async handle(data: CampaignDeliveryMessage): Promise<void> {
    const deliveryId = new Types.ObjectId(data.deliveryId);

    // Step 0: re-read. Truth source is the row, not the message. Must be IN_PROGRESS.
    const delivery = await this.deliveryService.findOne({ _id: deliveryId });
    if (!delivery || delivery.status !== DELIVERY_STATUS.IN_PROGRESS) return; // ack-skip

    const campaignId = delivery.campaignId as Types.ObjectId;

    // Step 0.5: epoch fence. A stale (pre-resume) message must not touch the row.
    const currentEpoch = await this.campaignService.getDispatchEpoch(campaignId);
    if (data.epoch < currentEpoch) return; // ack-skip, row untouched

    // Step 1: CAS IN_PROGRESS → SENDING. Concurrent loser → ack-skip.
    const cas = await this.deliveryService.markSendingIfInProgress(deliveryId);
    if (cas.modifiedCount !== 1) return;

    // Step 2: deliver stub — business logic stripped. Random success/failure.
    const ok = await this.deliverStub();

    // Step 3: finalize terminal status.
    await this.deliveryService.markTerminal({
      deliveryId,
      status: ok ? DELIVERY_STATUS.SUCCESS : DELIVERY_STATUS.FAILED,
      completedAt: new Date(),
      ...(ok ? {} : { errorMessage: 'stub_random_failure' }),
    });
  }

  // Placeholder for the real channels (SMS/Email/Mail). Kept intentionally trivial.
  private async deliverStub(): Promise<boolean> {
    await new Promise((r) => setTimeout(r, 50));
    return Math.random() > 0.1;
  }
}
```

- [ ] **Step 5: 写 `pulsar.module.ts`**

```ts
import { Module } from '@nestjs/common';
import { PulsarClientProvider } from './pulsar-client.provider';
import { CampaignDeliveryProducer } from './campaign-delivery.producer';
import { CampaignDeliveryConsumer } from './campaign-delivery.consumer';
import { CampaignDeliveryModule } from '../../modules/campaign-delivery/campaign-delivery.module';
import { CampaignModule } from '../../modules/campaign/campaign.module';

@Module({
  imports: [CampaignDeliveryModule, CampaignModule],
  providers: [PulsarClientProvider, CampaignDeliveryProducer, CampaignDeliveryConsumer],
  exports: [PulsarClientProvider, CampaignDeliveryProducer, CampaignDeliveryConsumer],
})
export class PulsarModule {}
```

- [ ] **Step 6: 编译确认**

Run: `cd backend && npx tsc --noEmit`
Expected: 通过。

- [ ] **Step 7: Commit**（需用户同意）

```bash
git add backend/src/libs/pulsar
git commit -m "feat: pulsar producer + shared-subscription consumer with epoch fence"
```

---

## Task 7: Temporal workflows + targeting activity

**Files:**
- Create: `backend/src/libs/temporal/workflow.ts`
- Create: `backend/src/interfaces/activities.interface.ts`
- Create: `backend/src/modules/campaign-temporal-scheduler/workflows/execute-campaign.workflow.ts`
- Create: `backend/src/modules/campaign-delivery-temporal/workflows/campaign-dispatcher.workflow.ts`
- Create: `backend/src/modules/campaign-temporal-scheduler/activities/targeting.activity.ts`

**Interfaces:**
- Consumes: `CampaignDeliveryService.createMany`
- Produces:
  - workflow `executeCampaignWorkflow(input: { campaignId: string; dispatcherTaskQueue: string })`
  - workflow `campaignDispatcherWorkflow(input: { campaignId: string; epoch: number; dispatchChunk: number; dispatcherConcurrency: number })`
  - activity 接口 `Targeting_Activity_Interface = { targeting(input:{campaignId:string}): Promise<void> }`
  - activity 接口 `DispatchPlayers_Activity_Interface`（Task 8 实现，签名在此声明）
  - `TargetingActivity` Nest provider，含 `targeting` 方法

- [ ] **Step 1: 写 `libs/temporal/workflow.ts`**

```ts
// Single import seam for the workflow sandbox. Workflows must only import from
// @temporalio/workflow (no Nest, no Node APIs) — re-exporting here documents that.
export {
  proxyActivities,
  startChild,
  ParentClosePolicy,
} from '@temporalio/workflow';
```

- [ ] **Step 2: 写 `interfaces/activities.interface.ts`**

```ts
export interface Targeting_Activity_Interface {
  targeting(input: { campaignId: string }): Promise<void>;
}

export interface DispatchPlayers_Activity_Interface {
  dispatchPlayers(input: {
    campaignId: string;
    epoch: number;
    dispatchChunk: number;
    dispatcherConcurrency: number;
  }): Promise<void>;
}

export interface ExecuteCampaignWorkflow_Input_Interface {
  campaignId: string;
  dispatcherTaskQueue: string;
}

export interface CampaignDispatcherWorkflow_Input_Interface {
  campaignId: string;
  epoch: number;
  dispatchChunk: number;
  dispatcherConcurrency: number;
}
```

- [ ] **Step 3: 写 `execute-campaign.workflow.ts`**

```ts
import { proxyActivities, startChild, ParentClosePolicy } from '../../../libs/temporal/workflow';
import {
  CAMPAIGN_DISPATCHER_WORKFLOW_TYPE,
  buildDispatcherWorkflowId,
} from '../../../libs/temporal/temporal.constants';
import type {
  Targeting_Activity_Interface,
  ExecuteCampaignWorkflow_Input_Interface,
} from '../../../interfaces/activities.interface';

const { targeting } = proxyActivities<Targeting_Activity_Interface>({
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, initialInterval: '5s', backoffCoefficient: 2 },
});

// Parent workflow: build the audience, then hand off to the dispatcher child.
export async function executeCampaignWorkflow(
  input: ExecuteCampaignWorkflow_Input_Interface,
): Promise<void> {
  const { campaignId, dispatcherTaskQueue } = input;

  await targeting({ campaignId });

  await startChild(CAMPAIGN_DISPATCHER_WORKFLOW_TYPE, {
    workflowId: buildDispatcherWorkflowId(campaignId),
    taskQueue: dispatcherTaskQueue,
    // ABANDON: parent completes immediately; without it the default TERMINATE
    // policy would kill the still-running dispatcher when the parent closes.
    parentClosePolicy: ParentClosePolicy.ABANDON,
    args: [{ campaignId, epoch: 0, dispatchChunk: 500, dispatcherConcurrency: 100 }],
  });
}
```

- [ ] **Step 4: 写 `campaign-dispatcher.workflow.ts`**

```ts
import { proxyActivities } from '../../../libs/temporal/workflow';
import type {
  DispatchPlayers_Activity_Interface,
  CampaignDispatcherWorkflow_Input_Interface,
} from '../../../interfaces/activities.interface';

const { dispatchPlayers } = proxyActivities<DispatchPlayers_Activity_Interface>({
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, initialInterval: '1s', backoffCoefficient: 2, maximumInterval: '1m' },
});

// Dispatcher's sole job: fan out delivery messages, then exit.
export async function campaignDispatcherWorkflow(
  input: CampaignDispatcherWorkflow_Input_Interface,
): Promise<void> {
  await dispatchPlayers(input);
}
```

- [ ] **Step 5: 写 `targeting.activity.ts`**

```ts
import { Injectable } from '@nestjs/common';
import { Types } from 'mongoose';
import { CampaignDeliveryService } from '../../campaign-delivery/campaign-delivery.service';
import { Targeting_Activity_Interface } from '../../../interfaces/activities.interface';

@Injectable()
export class TargetingActivity implements Targeting_Activity_Interface {
  constructor(private readonly deliveryService: CampaignDeliveryService) {}

  // Business targeting stripped: just materialise a fixed batch of PENDING rows.
  async targeting(input: { campaignId: string }): Promise<void> {
    await this.deliveryService.createMany(new Types.ObjectId(input.campaignId), 1000);
  }
}
```

- [ ] **Step 6: 编译确认**

Run: `cd backend && npx tsc --noEmit`
Expected: 通过。

- [ ] **Step 7: Commit**（需用户同意）

```bash
git add backend/src/libs/temporal/workflow.ts backend/src/interfaces/activities.interface.ts backend/src/modules/campaign-temporal-scheduler backend/src/modules/campaign-delivery-temporal/workflows
git commit -m "feat: temporal parent/child workflows + targeting activity"
```

---

## Task 8: dispatchPlayers activity（分页 + 逐行 CAS + 发 Pulsar）

**Files:**
- Create: `backend/src/modules/campaign-delivery-temporal/activities/dispatch-players.activity.ts`

**Interfaces:**
- Consumes: `ICampaignDeliveryDispatchPort`(token), `CampaignDeliveryProducer`, `RedisService`, `CampaignService`
- Produces: `DispatchPlayersActivity.dispatchPlayers(input)` 实现 `DispatchPlayers_Activity_Interface`

- [ ] **Step 1: 写 `dispatch-players.activity.ts`**

```ts
import { Inject, Injectable } from '@nestjs/common';
import { heartbeat } from '@temporalio/activity';
import { Types } from 'mongoose';
import { TOKENS, CAMPAIGN_STATUS } from '../../../constants';
import { ICampaignDeliveryDispatchPort } from '../../../interfaces/campaign-delivery.port';
import { DispatchPlayers_Activity_Interface } from '../../../interfaces/activities.interface';
import { CampaignDeliveryProducer } from '../../../libs/pulsar/campaign-delivery.producer';
import { RedisService } from '../../../libs/redis/redis.service';
import { CampaignService } from '../../campaign/campaign.service';

@Injectable()
export class DispatchPlayersActivity implements DispatchPlayers_Activity_Interface {
  constructor(
    @Inject(TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT)
    private readonly dispatchPort: ICampaignDeliveryDispatchPort,
    private readonly producer: CampaignDeliveryProducer,
    private readonly redis: RedisService,
    private readonly campaignService: CampaignService,
  ) {}

  async dispatchPlayers(input: {
    campaignId: string;
    epoch: number;
    dispatchChunk: number;
    dispatcherConcurrency: number;
  }): Promise<void> {
    const campaignId = new Types.ObjectId(input.campaignId);
    const { dispatchChunk, dispatcherConcurrency } = input;

    // Read the authoritative epoch once at dispatch start; stamp it on every message.
    const epoch = await this.campaignService.getDispatchEpoch(campaignId);

    // Claim campaign PENDING -> IN_PROGRESS. Idempotent: re-runs get modifiedCount 0.
    await this.campaignService.claimInProgressIfPending(campaignId);

    let processed = 0;
    for (;;) {
      if (await this.redis.isPaused(input.campaignId)) break; // early stop; epoch covers correctness

      const page = await this.dispatchPort.findPendingPage(campaignId, dispatchChunk);
      if (page.length === 0) break;

      await this.mapWithConcurrency(page, dispatcherConcurrency, async (delivery) => {
        // Mark-first: CAS PENDING->IN_PROGRESS, send ONLY on win. A row marked but
        // not sent (crash) becomes a ghost reaped by reconciliation; a row not yet
        // marked stays PENDING for the next dispatch round.
        const cas = await this.dispatchPort.markInProgressIfPending(delivery._id);
        if (cas.modifiedCount !== 1) return;
        await this.producer.send({
          deliveryId: delivery._id.toString(),
          campaignId: input.campaignId,
          epoch,
        });
      });

      processed += page.length;
      heartbeat({ processed });
    }
  }

  // Bounded-concurrency map: N workers drain a shared index.
  private async mapWithConcurrency<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
    let index = 0;
    const worker = async () => {
      while (index < items.length) {
        const i = index++;
        await fn(items[i]);
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  }
}
```

- [ ] **Step 2: 编译确认**

Run: `cd backend && npx tsc --noEmit`
Expected: 通过。

- [ ] **Step 3: Commit**（需用户同意）

```bash
git add backend/src/modules/campaign-delivery-temporal/activities/dispatch-players.activity.ts
git commit -m "feat: dispatchPlayers activity (paged CAS + pulsar fan-out)"
```

---

## Task 9: Reconciliation cron（TDD 纯逻辑 + 接线）

**Files:**
- Create: `backend/src/orchestration/reconciliation/reconciliation.service.ts`
- Create: `backend/src/orchestration/reconciliation/reconciliation.module.ts`
- Test: `backend/test/reconciliation.service.spec.ts`

**Interfaces:**
- Consumes: `CampaignService.listInProgress/markCompleted`, `CampaignDeliveryService.countByStatus`
- Produces: `ReconciliationService.reconcileOnce(): Promise<void>` + `@Cron` 包装

- [ ] **Step 1: 写失败测试 `test/reconciliation.service.spec.ts`**

```ts
import { ReconciliationService } from '../src/orchestration/reconciliation/reconciliation.service';
import { DELIVERY_STATUS } from '../src/constants';
import { Types } from 'mongoose';

describe('ReconciliationService', () => {
  it('marks campaign completed when no non-terminal deliveries remain', async () => {
    const id = new Types.ObjectId();
    const campaignService = {
      listInProgress: jest.fn().mockResolvedValue([{ _id: id }]),
      markCompleted: jest.fn().mockResolvedValue(undefined),
    };
    const deliveryService = {
      countByStatus: jest.fn().mockImplementation((_cid, status) =>
        status === DELIVERY_STATUS.PENDING || status === DELIVERY_STATUS.IN_PROGRESS || status === DELIVERY_STATUS.SENDING
          ? Promise.resolve(0) : Promise.resolve(5)),
    };
    const svc = new ReconciliationService(campaignService as any, deliveryService as any);
    await svc.reconcileOnce();
    expect(campaignService.markCompleted).toHaveBeenCalledWith(id);
  });

  it('does NOT complete while non-terminal deliveries remain', async () => {
    const id = new Types.ObjectId();
    const campaignService = {
      listInProgress: jest.fn().mockResolvedValue([{ _id: id }]),
      markCompleted: jest.fn(),
    };
    const deliveryService = {
      countByStatus: jest.fn().mockImplementation((_cid, status) =>
        status === DELIVERY_STATUS.IN_PROGRESS ? Promise.resolve(3) : Promise.resolve(0)),
    };
    const svc = new ReconciliationService(campaignService as any, deliveryService as any);
    await svc.reconcileOnce();
    expect(campaignService.markCompleted).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && npx jest reconciliation.service`
Expected: FAIL，未定义。

- [ ] **Step 3: 写 `reconciliation.service.ts`**

```ts
import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { CampaignService } from '../../modules/campaign/campaign.service';
import { CampaignDeliveryService } from '../../modules/campaign-delivery/campaign-delivery.service';
import { DELIVERY_STATUS } from '../../constants';

@Injectable()
export class ReconciliationService {
  constructor(
    private readonly campaignService: CampaignService,
    private readonly deliveryService: CampaignDeliveryService,
  ) {}

  @Cron(CronExpression.EVERY_10_SECONDS)
  async reconcile(): Promise<void> {
    await this.reconcileOnce();
  }

  // A campaign is COMPLETED once no delivery is still PENDING/IN_PROGRESS/SENDING.
  // This is the safety net that owns campaign completion — not the dispatcher.
  async reconcileOnce(): Promise<void> {
    const campaigns = await this.campaignService.listInProgress();
    for (const c of campaigns) {
      const nonTerminal =
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.PENDING)) +
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.IN_PROGRESS)) +
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.SENDING));
      if (nonTerminal === 0) {
        await this.campaignService.markCompleted(c._id);
      }
    }
  }
}
```

- [ ] **Step 4: 写 `reconciliation.module.ts`**

```ts
import { Module } from '@nestjs/common';
import { ReconciliationService } from './reconciliation.service';
import { CampaignModule } from '../../modules/campaign/campaign.module';
import { CampaignDeliveryModule } from '../../modules/campaign-delivery/campaign-delivery.module';

@Module({
  imports: [CampaignModule, CampaignDeliveryModule],
  providers: [ReconciliationService],
})
export class ReconciliationModule {}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cd backend && npx jest reconciliation.service`
Expected: PASS (2 用例)。

- [ ] **Step 6: Commit**（需用户同意）

```bash
git add backend/src/orchestration/reconciliation backend/test/reconciliation.service.spec.ts
git commit -m "feat: reconciliation cron owning campaign completion with tests"
```

---

## Task 10: app.module + main.ts（接线 worker + consumer + API）

**Files:**
- Create: `backend/src/app.module.ts`
- Create: `backend/src/libs/temporal/temporal-worker.bootstrap.ts`
- Create: `backend/src/main.ts`

**Interfaces:**
- Consumes: 全部模块 + 两个 activity provider + consumer
- Produces: 可运行进程（HTTP API + Temporal worker×2 task queue + Pulsar consumer + cron）

- [ ] **Step 1: 写 `app.module.ts`**

```ts
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ScheduleModule } from '@nestjs/schedule';
import { CampaignModule } from './modules/campaign/campaign.module';
import { CampaignDeliveryModule } from './modules/campaign-delivery/campaign-delivery.module';
import { PulsarModule } from './libs/pulsar/pulsar.module';
import { ReconciliationModule } from './orchestration/reconciliation/reconciliation.module';
import { TargetingActivity } from './modules/campaign-temporal-scheduler/activities/targeting.activity';
import { DispatchPlayersActivity } from './modules/campaign-delivery-temporal/activities/dispatch-players.activity';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? 'mongodb://localhost:27017/campaign_demo'),
    ScheduleModule.forRoot(),
    CampaignModule,
    CampaignDeliveryModule,
    PulsarModule,
    ReconciliationModule,
  ],
  // Activities are Nest providers so they get full DI; the worker binds their methods.
  providers: [TargetingActivity, DispatchPlayersActivity],
  exports: [TargetingActivity, DispatchPlayersActivity],
})
export class AppModule {}
```

- [ ] **Step 2: 写 `temporal-worker.bootstrap.ts`**

```ts
import { INestApplicationContext } from '@nestjs/common';
import { Worker, NativeConnection } from '@temporalio/worker';
import { TargetingActivity } from '../../modules/campaign-temporal-scheduler/activities/targeting.activity';
import { DispatchPlayersActivity } from '../../modules/campaign-delivery-temporal/activities/dispatch-players.activity';
import { SCHEDULER_TASK_QUEUE, DISPATCHER_TASK_QUEUE } from './temporal.constants';

// Two workers on two task queues, mirroring source topology (scheduler vs dispatcher).
// Activities are pulled from the Nest container so they keep their injected deps.
export async function startTemporalWorkers(app: INestApplicationContext): Promise<void> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });

  const targeting = app.get(TargetingActivity);
  const dispatch = app.get(DispatchPlayersActivity);

  const schedulerWorker = await Worker.create({
    connection,
    namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    taskQueue: SCHEDULER_TASK_QUEUE,
    workflowsPath: require.resolve('../../modules/campaign-temporal-scheduler/workflows/execute-campaign.workflow'),
    activities: { targeting: targeting.targeting.bind(targeting) },
  });

  const dispatcherWorker = await Worker.create({
    connection,
    namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    taskQueue: DISPATCHER_TASK_QUEUE,
    workflowsPath: require.resolve('../../modules/campaign-delivery-temporal/workflows/campaign-dispatcher.workflow'),
    activities: { dispatchPlayers: dispatch.dispatchPlayers.bind(dispatch) },
  });

  void schedulerWorker.run();
  void dispatcherWorker.run();
}
```

注：`execute-campaign.workflow` 通过 `startChild(CAMPAIGN_DISPATCHER_WORKFLOW_TYPE)` 按名调度子 workflow，子 workflow 注册在 dispatcherWorker 的 `workflowsPath`，两者按 type 名解耦，无需互相 import。

- [ ] **Step 3: 写 `main.ts`**

```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { startTemporalWorkers } from './libs/temporal/temporal-worker.bootstrap';
import { CampaignDeliveryConsumer } from './libs/pulsar/campaign-delivery.consumer';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(3000);

  // Start the Pulsar consume loop and the Temporal workers alongside the HTTP API.
  await app.get(CampaignDeliveryConsumer).start();
  await startTemporalWorkers(app);

  // eslint-disable-next-line no-console
  console.log('API on :3000, Temporal workers + Pulsar consumer running');
}
void bootstrap();
```

- [ ] **Step 4: 编译确认**

Run: `cd backend && npx tsc --noEmit`
Expected: 通过。

- [ ] **Step 5: 端到端冒烟（需 docker 起来）**

Run:
```bash
docker compose up -d
cd backend && npm run start:dev
# 另开终端：
curl -XPOST localhost:3000/campaigns                # → {"id":"..."}
curl -XPOST localhost:3000/campaigns/<id>/dispatch  # → {"started":true}
sleep 20
curl localhost:3000/campaigns/<id>                  # counts 应全部进入 SUCCESS/FAILED，campaign COMPLETED
```
Expected: Temporal UI (`localhost:8080`) 能看到 `executeCampaignWorkflow` + 子 `campaignDispatcherWorkflow`；最终 1000 条 delivery 全部终态，campaign `COMPLETED`。

- [ ] **Step 6: 暂停/恢复验证**

Run:
```bash
curl -XPOST localhost:3000/campaigns                # 新 campaign
curl -XPOST localhost:3000/campaigns/<id>/dispatch
curl -XPOST localhost:3000/campaigns/<id>/pause     # 立即暂停
curl localhost:3000/campaigns/<id>                  # 应有残留 PENDING
curl -XPOST localhost:3000/campaigns/<id>/resume    # epoch++，重新派发
sleep 20
curl localhost:3000/campaigns/<id>                  # 最终全部终态 + COMPLETED
```
Expected: resume 后 epoch 增加，残留 PENDING 被重新派发至终态。

- [ ] **Step 7: Commit**（需用户同意）

```bash
git add backend/src/app.module.ts backend/src/main.ts backend/src/libs/temporal/temporal-worker.bootstrap.ts
git commit -m "feat: wire temporal workers + pulsar consumer + api in main"
```

---

## Task 11: 前端纯静态讲解页

**Files:**
- Create: `frontend/index.html`, `frontend/styles.css`, `frontend/app.js`, `frontend/snippets.js`

**Interfaces:**
- Produces: 浏览器直接打开的单页交互架构图，零网络请求。

- [ ] **Step 1: 写 `snippets.js`（静态代码片段 + 讲解文案）**

```js
// Static, hand-curated snippets + rationale. No network — everything inlined.
window.NODES = [
  {
    id: 'execute',
    title: 'executeCampaignWorkflow (父 Workflow)',
    what: 'HTTP 触发后由 Temporal 调度。先跑 targeting 生成受众，再 startChild 派发子 workflow。',
    why: 'Temporal 提供持久化执行与自动重试：进程崩溃后 workflow 从上次状态续跑。ParentClosePolicy.ABANDON 让父完成后子继续运行。',
    code: `export async function executeCampaignWorkflow(input) {
  await targeting({ campaignId: input.campaignId });
  await startChild(CAMPAIGN_DISPATCHER_WORKFLOW_TYPE, {
    parentClosePolicy: ParentClosePolicy.ABANDON,
    args: [{ campaignId: input.campaignId, epoch: 0 }],
  });
}`,
  },
  {
    id: 'dispatch',
    title: 'dispatchPlayers (Activity)',
    what: '分页拉 PENDING delivery，逐行 CAS PENDING→IN_PROGRESS，命中才发 Pulsar 消息（带 epoch）。',
    why: 'Mark-first CAS 保证幂等：Temporal 重试不会重复发送。每页 heartbeat 让长任务可被监控与续跑。',
    code: `const cas = await markInProgressIfPending(delivery._id);
if (cas.modifiedCount !== 1) return;   // 已被并发/暂停占用，跳过
await producer.send({ deliveryId, campaignId, epoch });`,
  },
  {
    id: 'pulsar',
    title: 'Pulsar (Shared 订阅)',
    what: 'dispatchPlayers 作为 producer 投递，多个 consumer 以 Shared 订阅并行消费同一 backlog。',
    why: 'Shared 订阅水平扩展消费吞吐；ackTimeout + 不 ack 即重投，保证 at-least-once，配合下游 CAS 去重达成 effectively-once。',
    code: `subscriptionType: 'Shared',
ackTimeoutMs: 5 * 60 * 1000,
// 未 ack → Pulsar 超时后重投，绝不静默丢消息`,
  },
  {
    id: 'consumer',
    title: 'CampaignDeliveryConsumer',
    what: '重读 delivery → epoch 栅栏 → CAS IN_PROGRESS→SENDING → 空投递 → 写终态。',
    why: '消息只是触发器，真值源是 DB 行。epoch 栅栏丢弃暂停/rewind 前的陈旧消息，CAS 防并发重复处理。',
    code: `if (data.epoch < currentEpoch) return;          // epoch 栅栏
const cas = await markSendingIfInProgress(id);
if (cas.modifiedCount !== 1) return;            // 并发输者跳过
await deliverStub();
await markTerminal({ status: ok ? 'SUCCESS' : 'FAILED' });`,
  },
  {
    id: 'reconcile',
    title: 'Reconciliation Cron',
    what: '定时扫描 IN_PROGRESS campaign，当无非终态 delivery 时标记 COMPLETED。',
    why: '完成判定不放在 dispatcher（它可能因 ABANDON/崩溃提前退出），由独立 cron 兜底 → 最终一致性。',
    code: `if (nonTerminal === 0) {
  await campaignService.markCompleted(c._id);
}`,
  },
];

window.STATES = ['PENDING', 'IN_PROGRESS', 'SENDING', 'SUCCESS', 'FAILED'];
```

- [ ] **Step 2: 写 `index.html`**

```html
<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Campaign Dispatch 架构讲解</title>
  <link rel="stylesheet" href="styles.css" />
</head>
<body>
  <header>
    <h1>Campaign + Temporal + Pulsar 派发架构</h1>
    <p>点击任一节点查看「做什么 / 为什么这样设计 / 对应代码」。纯静态，无后端调用。</p>
  </header>

  <section id="flow" class="flow"><!-- nodes injected by app.js --></section>

  <section class="statemachine">
    <h2>Delivery 状态机（CAS 流转）</h2>
    <div id="states" class="states"></div>
  </section>

  <aside id="detail" class="detail">
    <h2 id="detail-title">点击上方节点</h2>
    <h3>做什么</h3><p id="detail-what"></p>
    <h3>为什么这样设计</h3><p id="detail-why"></p>
    <h3>对应代码</h3><pre><code id="detail-code"></code></pre>
  </aside>

  <script src="snippets.js"></script>
  <script src="app.js"></script>
</body>
</html>
```

- [ ] **Step 3: 写 `styles.css`**

```css
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, "Segoe UI", system-ui, sans-serif; color: #1c2128; background: #f6f8fa; }
header { padding: 24px 32px; background: #0d1117; color: #fff; }
header h1 { margin: 0 0 8px; font-size: 22px; }
header p { margin: 0; color: #9da7b3; font-size: 14px; }
.flow { display: flex; gap: 12px; align-items: center; padding: 32px; overflow-x: auto; }
.node { flex: 0 0 auto; min-width: 170px; padding: 14px 16px; background: #fff; border: 2px solid #d0d7de; border-radius: 10px; cursor: pointer; transition: .15s; }
.node:hover { border-color: #0969da; transform: translateY(-2px); }
.node.active { border-color: #0969da; box-shadow: 0 0 0 3px rgba(9,105,218,.2); }
.node h4 { margin: 0; font-size: 14px; }
.arrow { color: #57606a; font-size: 20px; }
.statemachine { padding: 0 32px 24px; }
.states { display: flex; gap: 8px; flex-wrap: wrap; }
.state { padding: 8px 14px; border-radius: 20px; background: #fff; border: 1px solid #d0d7de; font-size: 13px; animation: pulse 2s infinite; }
@keyframes pulse { 0%,100% { opacity: .55; } 50% { opacity: 1; } }
.detail { margin: 0 32px 40px; padding: 20px 24px; background: #fff; border: 1px solid #d0d7de; border-radius: 10px; }
.detail h3 { margin: 16px 0 4px; font-size: 13px; color: #0969da; text-transform: uppercase; letter-spacing: .04em; }
.detail pre { background: #0d1117; color: #e6edf3; padding: 16px; border-radius: 8px; overflow-x: auto; font-size: 13px; }
```

- [ ] **Step 4: 写 `app.js`**

```js
// Render the flow nodes and wire click → detail panel. Pure DOM, no fetch.
(function () {
  const flow = document.getElementById('flow');
  window.NODES.forEach((node, i) => {
    if (i > 0) {
      const a = document.createElement('span');
      a.className = 'arrow';
      a.textContent = '→';
      flow.appendChild(a);
    }
    const el = document.createElement('div');
    el.className = 'node';
    el.innerHTML = '<h4>' + node.title + '</h4>';
    el.addEventListener('click', () => select(node, el));
    flow.appendChild(el);
  });

  const states = document.getElementById('states');
  window.STATES.forEach((s) => {
    const el = document.createElement('span');
    el.className = 'state';
    el.textContent = s;
    states.appendChild(el);
  });

  function select(node, el) {
    document.querySelectorAll('.node').forEach((n) => n.classList.remove('active'));
    el.classList.add('active');
    document.getElementById('detail-title').textContent = node.title;
    document.getElementById('detail-what').textContent = node.what;
    document.getElementById('detail-why').textContent = node.why;
    document.getElementById('detail-code').textContent = node.code;
  }

  if (window.NODES.length) select(window.NODES[0], flow.querySelector('.node'));
})();
```

- [ ] **Step 5: 浏览器验证**

Run: `open frontend/index.html`（macOS）
Expected: 页面打开；5 个节点横向带箭头连接；点击切换右下详情与代码；DevTools Network 面板零请求。

- [ ] **Step 6: Commit**（需用户同意）

```bash
git add frontend
git commit -m "feat: static single-page architecture walkthrough"
```

---

## Task 12: README + 全量测试收尾

**Files:**
- Create: `README.md`

**Interfaces:** 无新代码接口。

- [ ] **Step 1: 写 `README.md`**

包含：项目目的（面试 demo）、架构图 ASCII、`docker compose up -d` + `cd backend && npm i && npm run start:dev`、curl 示例（create/dispatch/pause/resume/get）、Temporal UI 与 Pulsar 端口、前端 `open frontend/index.html`、设计亮点清单（CAS 幂等 / epoch 栅栏 / Shared 订阅 at-least-once / reconciliation 兜底 / 六边形端口）。

- [ ] **Step 2: 跑全部单元测试**

Run: `cd backend && npx jest`
Expected: 全 PASS（campaign-delivery / redis / reconciliation 共 7 用例）。

- [ ] **Step 3: Commit**（需用户同意）

```bash
git add README.md
git commit -m "docs: add demo readme + run instructions"
```

---

## Self-Review 结果

**Spec 覆盖**：主链路(T7/T8/T10)、CAS(T3)、epoch 栅栏(T6 consumer + T5 epoch)、暂停/恢复(T4/T5)、对账 cron(T9)、docker 全家桶(T1)、前端单页(T11)、可运行验收(T10 S5-6)、前端验收(T11 S5) 均有任务覆盖。recurring/grant/budget/三通道 已按 spec 剥离。

**占位符扫描**：无 TBD/TODO；`deliverStub` 是 spec 明确要求的空投递实现，非占位符。

**类型一致性**：`markInProgressIfPending`/`markSendingIfInProgress`/`markTerminal`/`findPendingPage`/`getDispatchEpoch`/`bumpEpoch`/`countByStatus`/`listInProgress`/`markCompleted` 在定义任务(T3/T5)与消费任务(T6/T8/T9)间签名一致。workflow type 常量(T5)与 worker 注册(T10)、startChild 调用(T7)一致。
