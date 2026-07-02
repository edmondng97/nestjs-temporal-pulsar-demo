# Three-Part Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the repo into `backend/` (add SSE + list endpoint + CORS), `frontend/` (new Vite + React runtime console), and `demo/` (dark-tech keynote-style static presentation page).

**Architecture:** Backend gains a broadcast `CampaignEventsService` (RxJS Subject) that the Pulsar consumer pushes per-delivery events into and a `@Sse()` controller endpoint streams out. Frontend is a Vite + React + TS console (list + detail + embedded SSE event log, 2s polling for state). Demo is a zero-build static keynote (8 slides, keyboard nav, CSS-only animations).

**Tech Stack:** NestJS 10 (rxjs already a dependency), Vite + React 18 + TypeScript, vanilla HTML/CSS/JS for demo.

**Spec:** `docs/superpowers/specs/2026-07-02-three-part-split-design.md`

## Global Constraints

- Backend: NO new npm dependencies (rxjs ships with NestJS). Only additions allowed: events service/module, SSE endpoint, `GET /campaigns` list, `paused`/`dispatchEpoch` in responses, CORS. No other backend logic changes.
- Frontend: Vite + React + TypeScript only. NO UI component library, NO state-management library, NO animation library.
- Demo: pure static HTML/CSS/JS, zero build, must open via `file://` double-click.
- Delivery terminal statuses are `SUCCESS` / `FAILED` (there is no `SENT` status). Stream shows SUCCESS green, stale-epoch REJECTED red.
- Campaign statuses: `PENDING` / `IN_PROGRESS` / `COMPLETED`. "Paused" is a Redis flag exposed as boolean `paused`, not a status value.
- `POST /campaigns` takes no body — the console's "New Campaign" is a plain button (no form/modal).
- Code comments in English. Working dir for backend commands: `backend/`.
- Existing test suites (3 suites / 7 tests) must stay green after every backend task: `cd backend && npx jest`.
- Do NOT prefix build commands with rtk (known rewrite bug): run `npm run build` exactly.
- Commit after each task (branch: `feature/three-part-split` off current `feature/campaign-dispatch-demo`).

---

### Task 1: CampaignEventsService (broadcast hub)

**Files:**
- Create: `backend/src/libs/events/campaign-events.service.ts`
- Create: `backend/src/libs/events/campaign-events.module.ts`
- Test: `backend/test/campaign-events.service.spec.ts`

**Interfaces:**
- Consumes: nothing (leaf provider).
- Produces: `CampaignEventsService.emit(event: CampaignDeliveryEvent): void` and `CampaignEventsService.stream(): Observable<CampaignDeliveryEvent>`; type `CampaignDeliveryEvent = { campaignId: string; deliveryId: string; outcome: 'SUCCESS' | 'FAILED' | 'REJECTED_STALE'; epoch: number; ts: string }`; `CampaignEventsModule` exporting the service.

- [ ] **Step 1: Write the failing test**

```typescript
// backend/test/campaign-events.service.spec.ts
import { CampaignEventsService, CampaignDeliveryEvent } from '../src/libs/events/campaign-events.service';

describe('CampaignEventsService', () => {
  it('delivers emitted events to subscribers', () => {
    const svc = new CampaignEventsService();
    const seen: CampaignDeliveryEvent[] = [];
    const sub = svc.stream().subscribe((e) => seen.push(e));
    const event: CampaignDeliveryEvent = {
      campaignId: 'c1', deliveryId: 'd1', outcome: 'SUCCESS', epoch: 0, ts: '2026-07-02T00:00:00.000Z',
    };
    svc.emit(event);
    sub.unsubscribe();
    expect(seen).toEqual([event]);
  });

  it('does not replay past events to late subscribers', () => {
    const svc = new CampaignEventsService();
    svc.emit({ campaignId: 'c1', deliveryId: 'd1', outcome: 'FAILED', epoch: 1, ts: 't' });
    const seen: CampaignDeliveryEvent[] = [];
    const sub = svc.stream().subscribe((e) => seen.push(e));
    sub.unsubscribe();
    expect(seen).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && npx jest test/campaign-events.service.spec.ts`
Expected: FAIL — cannot find module '../src/libs/events/campaign-events.service'

- [ ] **Step 3: Write the service + module**

```typescript
// backend/src/libs/events/campaign-events.service.ts
import { Injectable } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';

export interface CampaignDeliveryEvent {
  campaignId: string;
  deliveryId: string;
  outcome: 'SUCCESS' | 'FAILED' | 'REJECTED_STALE';
  epoch: number;
  ts: string;
}

// Plain Subject (not Replay): SSE clients only care about live traffic.
@Injectable()
export class CampaignEventsService {
  private readonly subject = new Subject<CampaignDeliveryEvent>();

  emit(event: CampaignDeliveryEvent): void {
    this.subject.next(event);
  }

  stream(): Observable<CampaignDeliveryEvent> {
    return this.subject.asObservable();
  }
}
```

```typescript
// backend/src/libs/events/campaign-events.module.ts
import { Module } from '@nestjs/common';
import { CampaignEventsService } from './campaign-events.service';

// Single shared instance: both the Pulsar consumer (producer side) and the
// campaign controller (SSE side) must see the SAME Subject.
@Module({
  providers: [CampaignEventsService],
  exports: [CampaignEventsService],
})
export class CampaignEventsModule {}
```

- [ ] **Step 4: Run tests to verify pass (full suite)**

Run: `cd backend && npx jest`
Expected: all suites PASS (previous 7 tests + 2 new)

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/three-part-split
git add backend/src/libs/events backend/test/campaign-events.service.spec.ts
git commit -m "feat: campaign events broadcast service"
```

---

### Task 2: Consumer emits delivery events

**Files:**
- Modify: `backend/src/libs/pulsar/campaign-delivery.consumer.ts` (constructor + `handle()`)
- Modify: `backend/src/libs/pulsar/pulsar.module.ts` (import `CampaignEventsModule`)
- Test: existing consumer spec under `backend/test/` (locate with `ls backend/test`); update its constructor call sites

**Interfaces:**
- Consumes: `CampaignEventsService.emit()` from Task 1.
- Produces: events emitted at exactly three points in `handle()`: stale fence hit → `REJECTED_STALE`; terminal success → `SUCCESS`; terminal failure → `FAILED`. No events for ack-skip (row missing / not IN_PROGRESS) or CAS-lose paths.

- [ ] **Step 1: Update the consumer**

Add to imports and constructor:

```typescript
import { CampaignEventsService } from '../events/campaign-events.service';
// constructor gains:
    private readonly events: CampaignEventsService,
```

In `handle()`, replace the epoch-fence return and the finalize step:

```typescript
    // Step 0.5: epoch fence. A stale (pre-resume) message must not touch the row.
    const currentEpoch = await this.campaignService.getDispatchEpoch(campaignId);
    if (data.epoch < currentEpoch) {
      this.events.emit({
        campaignId: campaignId.toString(),
        deliveryId: data.deliveryId,
        outcome: 'REJECTED_STALE',
        epoch: data.epoch,
        ts: new Date().toISOString(),
      });
      return; // ack-skip, row untouched
    }
```

```typescript
    // Step 3: finalize terminal status.
    const status = ok ? DELIVERY_STATUS.SUCCESS : DELIVERY_STATUS.FAILED;
    await this.deliveryService.markTerminal({
      deliveryId,
      status,
      completedAt: new Date(),
      ...(ok ? {} : { errorMessage: 'stub_random_failure' }),
    });
    this.events.emit({
      campaignId: campaignId.toString(),
      deliveryId: data.deliveryId,
      outcome: ok ? 'SUCCESS' : 'FAILED',
      epoch: data.epoch,
      ts: new Date().toISOString(),
    });
```

In `backend/src/libs/pulsar/pulsar.module.ts`, add `CampaignEventsModule` to the `imports` array (`import { CampaignEventsModule } from '../events/campaign-events.module';`).

- [ ] **Step 2: Fix the existing consumer spec**

Run `cd backend && npx jest` first — the consumer spec will fail to construct the consumer (new constructor arg). In that spec, add `new CampaignEventsService()` (or a stub `{ emit: jest.fn() }` cast, matching the spec's existing style) as the new final constructor argument. If the spec asserts on the stale-fence path, also assert `emit` was called with `outcome: 'REJECTED_STALE'`.

- [ ] **Step 3: Run full suite**

Run: `cd backend && npx jest`
Expected: all PASS. Also: `cd backend && npx tsc --noEmit` → exit 0.

- [ ] **Step 4: Commit**

```bash
git add backend/src backend/test
git commit -m "feat: consumer emits delivery events (success/failed/stale-rejected)"
```

---

### Task 3: SSE endpoint, campaign list API, paused flag, CORS

**Files:**
- Modify: `backend/src/modules/campaign/campaign.controller.ts`
- Modify: `backend/src/modules/campaign/campaign.service.ts` (add `findAll()`)
- Modify: `backend/src/modules/campaign/campaign.module.ts` (import `CampaignEventsModule`)
- Modify: `backend/src/main.ts` (CORS)

**Interfaces:**
- Consumes: `CampaignEventsService.stream()` from Task 1.
- Produces (frontend contract, Tasks 5–8 depend on these exact shapes):
  - `GET /campaigns` → `Array<{ id: string; status: string; dispatchEpoch: number; paused: boolean; createdAt: string }>`
  - `GET /campaigns/:id` → `{ campaign, counts: { PENDING, IN_PROGRESS, SENDING, SUCCESS, FAILED }, paused: boolean }`
  - `GET /campaigns/events` → SSE stream, each message `data:` = JSON `CampaignDeliveryEvent`
  - `POST /campaigns` / `POST /campaigns/:id/dispatch|pause|resume` unchanged

- [ ] **Step 1: Add `findAll` to CampaignService**

```typescript
  async findAll(): Promise<CampaignDocument[]> {
    return this.model.find().sort({ createdAt: -1 }).exec();
  }
```

- [ ] **Step 2: Update the controller**

Add imports:

```typescript
import { Sse, MessageEvent } from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { CampaignEventsService } from '../../libs/events/campaign-events.service';
```

Inject `private readonly events: CampaignEventsService` in the constructor. Add routes — **`@Sse('events')` MUST be declared before `@Get(':id')`**, otherwise Nest matches `events` as an `:id`:

```typescript
  // SSE stream of per-delivery consumer outcomes. Declared before ':id' routes
  // so the literal path wins route matching.
  @Sse('events')
  sse(): Observable<MessageEvent> {
    return this.events.stream().pipe(map((e) => ({ data: e })));
  }

  @Get()
  async list() {
    const docs = await this.campaignService.findAll();
    return Promise.all(
      docs.map(async (d) => ({
        id: d._id.toString(),
        status: d.status,
        dispatchEpoch: d.dispatchEpoch,
        paused: await this.redis.isPaused(d._id.toString()),
        createdAt: (d as any).createdAt,
      })),
    );
  }
```

In the existing `get(':id')` handler, add `paused` to the return:

```typescript
    return { campaign, counts, paused: await this.redis.isPaused(id) };
```

Add `CampaignEventsModule` to `CampaignModule` imports.

- [ ] **Step 3: Enable CORS in main.ts**

```typescript
  const app = await NestFactory.create(AppModule);
  // Console dev server (Vite default port). Demo page is static and calls no API.
  app.enableCors({ origin: ['http://localhost:5173'] });
```

- [ ] **Step 4: Compile + full suite**

Run: `cd backend && npx tsc --noEmit && npx jest`
Expected: exit 0, all tests PASS.

- [ ] **Step 5: Manual e2e verification (requires Docker stack)**

```bash
docker compose up -d          # repo root
cd backend && npm run build && node dist/main.js &
curl -s localhost:3000/campaigns            # → []
CID=$(curl -s -X POST localhost:3000/campaigns | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -N localhost:3000/campaigns/events &   # keep streaming in background
curl -s -X POST localhost:3000/campaigns/$CID/dispatch
```

Expected: the `curl -N` stream prints `data: {"campaignId":...,"outcome":"SUCCESS"...}` lines as deliveries drain. Then pause + resume and confirm `REJECTED_STALE` events appear. Tear down: kill node, `docker compose down`.

- [ ] **Step 6: Commit**

```bash
git add backend/src
git commit -m "feat: SSE events endpoint, campaign list API, paused flag, CORS"
```

---

### Task 4: Move static page to demo/

**Files:**
- Move: `frontend/index.html`, `frontend/app.js`, `frontend/snippets.js`, `frontend/styles.css` → `demo/`

**Interfaces:**
- Consumes: nothing.
- Produces: empty `frontend/` path free for the Vite scaffold (Task 5); `demo/` containing the old walkthrough as raw material for Tasks 9–11 (notably `demo/snippets.js` code snippets).

- [ ] **Step 1: git mv**

```bash
git mv frontend demo
git commit -m "refactor: move static walkthrough to demo/ (frontend/ reserved for React console)"
```

---

### Task 5: Frontend scaffold — Vite + React + TS, theme, API client

**Files:**
- Create: `frontend/` via Vite scaffold (`package.json`, `vite.config.ts`, `tsconfig.json`, `index.html`, `src/main.tsx`)
- Create: `frontend/src/App.tsx`, `frontend/src/theme.css`, `frontend/src/api.ts`, `frontend/.env.example`
- Delete: Vite template boilerplate (`src/App.css`, `src/assets/`, demo counter code)

**Interfaces:**
- Consumes: backend contract from Task 3.
- Produces (used by Tasks 6–8):

```typescript
// api.ts exports
export interface CampaignSummary { id: string; status: string; dispatchEpoch: number; paused: boolean; createdAt: string }
export interface CampaignDetail { campaign: { _id: string; status: string; dispatchEpoch: number }; counts: Record<'PENDING'|'IN_PROGRESS'|'SENDING'|'SUCCESS'|'FAILED', number>; paused: boolean }
export interface DeliveryEvent { campaignId: string; deliveryId: string; outcome: 'SUCCESS'|'FAILED'|'REJECTED_STALE'; epoch: number; ts: string }
export const API_URL: string
export function listCampaigns(): Promise<CampaignSummary[]>
export function getCampaign(id: string): Promise<CampaignDetail>
export function createCampaign(): Promise<{ id: string }>
export function dispatchCampaign(id: string): Promise<void>
export function pauseCampaign(id: string): Promise<void>
export function resumeCampaign(id: string): Promise<void>
```

- [ ] **Step 1: Scaffold**

```bash
cd /Users/edmond/Desktop/nestjs-temporal-pulsar-demo
npm create vite@latest frontend -- --template react-ts
cd frontend && npm install
rm -rf src/assets src/App.css
```

- [ ] **Step 2: API client**

```typescript
// frontend/src/api.ts
export const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

export interface CampaignSummary {
  id: string; status: string; dispatchEpoch: number; paused: boolean; createdAt: string;
}
export interface CampaignDetail {
  campaign: { _id: string; status: string; dispatchEpoch: number };
  counts: Record<'PENDING' | 'IN_PROGRESS' | 'SENDING' | 'SUCCESS' | 'FAILED', number>;
  paused: boolean;
}
export interface DeliveryEvent {
  campaignId: string; deliveryId: string;
  outcome: 'SUCCESS' | 'FAILED' | 'REJECTED_STALE';
  epoch: number; ts: string;
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, init);
  if (!res.ok) throw new Error(`${init?.method ?? 'GET'} ${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

export const listCampaigns = () => req<CampaignSummary[]>('/campaigns');
export const getCampaign = (id: string) => req<CampaignDetail>(`/campaigns/${id}`);
export const createCampaign = () => req<{ id: string }>('/campaigns', { method: 'POST' });
export const dispatchCampaign = (id: string) => req<void>(`/campaigns/${id}/dispatch`, { method: 'POST' });
export const pauseCampaign = (id: string) => req<void>(`/campaigns/${id}/pause`, { method: 'POST' });
export const resumeCampaign = (id: string) => req<void>(`/campaigns/${id}/resume`, { method: 'POST' });
```

```bash
# frontend/.env.example
echo 'VITE_API_URL=http://localhost:3000' > .env.example
```

- [ ] **Step 3: Theme CSS (dark tech, shared visual language with demo)**

```css
/* frontend/src/theme.css */
:root {
  --bg: #0a0e1a; --panel: #111827; --border: #1f2937;
  --text: #e5e7eb; --muted: #94a3b8;
  --accent: #22d3ee; --accent2: #818cf8;
  --green: #4ade80; --yellow: #fbbf24; --red: #f87171; --blue: #60a5fa;
  --mono: ui-monospace, SFMono-Regular, Menlo, monospace;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text);
  font: 14px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif; }
button { font: inherit; cursor: pointer; border-radius: 6px; border: 1px solid var(--border);
  background: var(--panel); color: var(--text); padding: 6px 14px; }
button:disabled { opacity: .4; cursor: not-allowed; }
.badge { padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
```

- [ ] **Step 4: App shell placeholder**

```tsx
// frontend/src/App.tsx
import './theme.css';

export default function App() {
  return (
    <div style={{ padding: 24 }}>
      <h1 style={{ margin: 0, fontSize: 18 }}>⚡ Campaign Console</h1>
      <p style={{ color: 'var(--muted)' }}>Connecting panels come in the next tasks.</p>
    </div>
  );
}
```

Update `src/main.tsx` to only render `<App />` (drop `index.css` import if the template added one; keep or remove `index.css` consistently).

- [ ] **Step 5: Verify build + dev server**

Run: `cd frontend && npx tsc --noEmit && npm run build`
Expected: exit 0, `dist/` produced. Optionally `npm run dev` and load http://localhost:5173.

- [ ] **Step 6: Commit**

```bash
git add frontend
git commit -m "feat: scaffold Vite+React console with dark theme and typed API client"
```

---

### Task 6: Campaign list panel + create + polling

**Files:**
- Create: `frontend/src/usePolling.ts`
- Create: `frontend/src/CampaignList.tsx`
- Modify: `frontend/src/App.tsx`

**Interfaces:**
- Consumes: `listCampaigns`, `createCampaign`, `CampaignSummary` from Task 5.
- Produces: `usePolling<T>(fn: () => Promise<T>, ms: number): { data: T | null; error: string | null }`; `<CampaignList campaigns selectedId onSelect onError />`; App state `selectedId: string | null` + `errorBar: string | null` (Tasks 7–8 plug into the same App layout slots).

- [ ] **Step 1: Polling hook**

```typescript
// frontend/src/usePolling.ts
import { useEffect, useRef, useState } from 'react';

// Poll fn every ms. Keeps last good data; surfaces the latest error separately
// so a transient API failure doesn't blank the UI.
export function usePolling<T>(fn: () => Promise<T>, ms: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const d = await fnRef.current();
        if (alive) { setData(d); setError(null); }
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    };
    void tick();
    const t = setInterval(tick, ms);
    return () => { alive = false; clearInterval(t); };
  }, [ms]);

  return { data, error };
}
```

- [ ] **Step 2: List component**

```tsx
// frontend/src/CampaignList.tsx
import { CampaignSummary, createCampaign } from './api';

const DOT: Record<string, string> = {
  PENDING: 'var(--muted)', IN_PROGRESS: 'var(--green)', COMPLETED: 'var(--blue)',
};

export function CampaignList(props: {
  campaigns: CampaignSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onError: (msg: string) => void;
}) {
  const onCreate = async () => {
    try { const { id } = await createCampaign(); props.onSelect(id); }
    catch (e) { props.onError((e as Error).message); }
  };

  return (
    <aside style={{ width: 260, borderRight: '1px solid var(--border)', padding: 12, overflowY: 'auto' }}>
      <button onClick={onCreate} style={{ width: '100%', marginBottom: 12, borderColor: 'var(--accent)', color: 'var(--accent)' }}>
        + New Campaign
      </button>
      {props.campaigns.map((c) => {
        const color = c.paused ? 'var(--yellow)' : (DOT[c.status] ?? 'var(--muted)');
        const selected = c.id === props.selectedId;
        return (
          <div key={c.id} onClick={() => props.onSelect(c.id)}
            style={{
              padding: 10, borderRadius: 6, marginBottom: 6, cursor: 'pointer',
              background: selected ? '#1e293b' : 'var(--panel)',
              borderLeft: `2px solid ${selected ? 'var(--accent)' : 'transparent'}`,
            }}>
            <div style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>{c.id.slice(-8)}</div>
            <div style={{ fontSize: 12, color }}>
              ● {c.paused ? 'PAUSED' : c.status} · epoch {c.dispatchEpoch}
            </div>
          </div>
        );
      })}
      {props.campaigns.length === 0 && <p style={{ color: 'var(--muted)', fontSize: 13 }}>No campaigns yet.</p>}
    </aside>
  );
}
```

- [ ] **Step 3: Wire into App**

```tsx
// frontend/src/App.tsx
import { useState } from 'react';
import './theme.css';
import { listCampaigns } from './api';
import { usePolling } from './usePolling';
import { CampaignList } from './CampaignList';

export default function App() {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [errorBar, setErrorBar] = useState<string | null>(null);
  const { data: campaigns, error: listError } = usePolling(listCampaigns, 2000);
  const err = errorBar ?? listError;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
      <header style={{ padding: '10px 16px', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', gap: 12 }}>
        <h1 style={{ margin: 0, fontSize: 16 }}>⚡ Campaign Console</h1>
        {err && (
          <span style={{ background: '#7f1d1d', color: '#fecaca', padding: '3px 10px', borderRadius: 6, fontSize: 12 }}
            onClick={() => setErrorBar(null)}>
            {err}
          </span>
        )}
      </header>
      <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        <CampaignList campaigns={campaigns ?? []} selectedId={selectedId}
          onSelect={setSelectedId} onError={setErrorBar} />
        <main style={{ flex: 1, padding: 16 }}>
          {selectedId
            ? <p style={{ color: 'var(--muted)' }}>Detail panel arrives in the next task.</p>
            : <p style={{ color: 'var(--muted)' }}>Select or create a campaign.</p>}
        </main>
      </div>
    </div>
  );
}
```

- [ ] **Step 4: Verify against live backend**

Backend + Docker up (as Task 3 Step 5). Run `cd frontend && npm run dev`, open http://localhost:5173: create a campaign via button → appears in list within 2s; kill backend → red error bar appears; restart → recovers.

- [ ] **Step 5: Typecheck, build, commit**

```bash
cd frontend && npx tsc --noEmit && npm run build
git add frontend/src
git commit -m "feat: campaign list panel with create button and 2s polling"
```

---

### Task 7: Detail panel — status, epoch, progress, counts, actions

**Files:**
- Create: `frontend/src/CampaignDetail.tsx`
- Modify: `frontend/src/App.tsx` (replace detail placeholder)

**Interfaces:**
- Consumes: `getCampaign`, `dispatchCampaign`, `pauseCampaign`, `resumeCampaign`, `usePolling`.
- Produces: `<CampaignDetail id onError>{eventStreamSlot}</CampaignDetail>` — renders children at the bottom of the panel; Task 8 passes the event stream component as children.

- [ ] **Step 1: Detail component**

```tsx
// frontend/src/CampaignDetail.tsx
import { ReactNode } from 'react';
import { getCampaign, dispatchCampaign, pauseCampaign, resumeCampaign } from './api';
import { usePolling } from './usePolling';

const STATUS_BG: Record<string, string> = {
  PENDING: 'var(--muted)', IN_PROGRESS: 'var(--green)', COMPLETED: 'var(--blue)',
};
const COUNT_COLORS: Record<string, string> = {
  PENDING: 'var(--muted)', IN_PROGRESS: 'var(--blue)', SENDING: 'var(--accent2)',
  SUCCESS: 'var(--green)', FAILED: 'var(--red)',
};

export function CampaignDetail(props: { id: string; onError: (m: string) => void; children?: ReactNode }) {
  const { data, error } = usePolling(() => getCampaign(props.id), 2000);
  if (error && !data) return <p style={{ color: 'var(--red)' }}>{error}</p>;
  if (!data) return <p style={{ color: 'var(--muted)' }}>Loading…</p>;

  const { campaign, counts, paused } = data;
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  const terminal = counts.SUCCESS + counts.FAILED;
  const pct = total ? Math.round((terminal / total) * 100) : 0;
  const status = paused ? 'PAUSED' : campaign.status;

  const act = (fn: (id: string) => Promise<void>) => () =>
    fn(props.id).catch((e) => props.onError((e as Error).message));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 12 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <span className="badge" style={{ background: paused ? 'var(--yellow)' : (STATUS_BG[campaign.status] ?? 'var(--muted)'), color: '#0a0e1a' }}>
          {status}
        </span>
        <span style={{ color: 'var(--muted)', fontSize: 13 }}>epoch {campaign.dispatchEpoch}</span>
        <span style={{ fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--muted)' }}>{props.id}</span>
      </div>

      <div>
        <div style={{ height: 8, background: 'var(--border)', borderRadius: 4 }}>
          <div style={{ width: `${pct}%`, height: '100%', borderRadius: 4,
            background: 'linear-gradient(90deg, var(--accent), var(--accent2))', transition: 'width .5s' }} />
        </div>
        <div style={{ display: 'flex', gap: 14, marginTop: 6, fontSize: 12 }}>
          <span style={{ color: 'var(--muted)' }}>{terminal}/{total} terminal ({pct}%)</span>
          {(Object.keys(counts) as Array<keyof typeof counts>).map((k) => (
            <span key={k} style={{ color: COUNT_COLORS[k] }}>{k} {counts[k]}</span>
          ))}
        </div>
      </div>

      <div style={{ display: 'flex', gap: 8 }}>
        <button onClick={act(dispatchCampaign)} disabled={campaign.status !== 'PENDING'}
          style={{ borderColor: 'var(--accent)', color: 'var(--accent)' }}>▶ Dispatch</button>
        <button onClick={act(pauseCampaign)} disabled={campaign.status !== 'IN_PROGRESS' || paused}
          style={{ borderColor: 'var(--yellow)', color: 'var(--yellow)' }}>⏸ Pause</button>
        <button onClick={act(resumeCampaign)} disabled={!paused}
          style={{ borderColor: 'var(--green)', color: 'var(--green)' }}>⏵ Resume</button>
      </div>

      {props.children /* live event stream slot (Task 8) */}
    </div>
  );
}
```

- [ ] **Step 2: Replace App placeholder**

In `App.tsx` `<main>`:

```tsx
          {selectedId
            ? <CampaignDetail id={selectedId} onError={setErrorBar} />
            : <p style={{ color: 'var(--muted)' }}>Select or create a campaign.</p>}
```

(with `import { CampaignDetail } from './CampaignDetail';`)

- [ ] **Step 3: Manual verify full loop**

With stack + backend up: create → Dispatch (button enabled only while PENDING) → progress bar advances, counts move; Pause → badge PAUSED, Resume enabled; Resume → epoch increments, drains to COMPLETED.

- [ ] **Step 4: Typecheck, build, commit**

```bash
cd frontend && npx tsc --noEmit && npm run build
git add frontend/src
git commit -m "feat: campaign detail panel with progress, counts, and actions"
```

---

### Task 8: Live event stream (SSE) + connection indicator

**Files:**
- Create: `frontend/src/EventStream.tsx`
- Modify: `frontend/src/App.tsx` (pass stream as CampaignDetail children)

**Interfaces:**
- Consumes: `DeliveryEvent`, `API_URL` from Task 5; children slot from Task 7.
- Produces: `<EventStream campaignId />` — subscribes once per mount to `${API_URL}/campaigns/events`, filters client-side by campaignId, caps buffer at 200 lines.

- [ ] **Step 1: EventStream component**

```tsx
// frontend/src/EventStream.tsx
import { useEffect, useRef, useState } from 'react';
import { API_URL, DeliveryEvent } from './api';

const OUTCOME_COLOR: Record<DeliveryEvent['outcome'], string> = {
  SUCCESS: 'var(--green)', FAILED: 'var(--red)', REJECTED_STALE: 'var(--red)',
};

export function EventStream(props: { campaignId: string }) {
  const [events, setEvents] = useState<DeliveryEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setEvents([]);
    const es = new EventSource(`${API_URL}/campaigns/events`);
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false); // EventSource auto-reconnects
    es.onmessage = (m) => {
      const e = JSON.parse(m.data) as DeliveryEvent;
      if (e.campaignId !== props.campaignId) return;
      setEvents((prev) => [...prev.slice(-199), e]); // cap at 200 lines
    };
    return () => es.close();
  }, [props.campaignId]);

  useEffect(() => {
    boxRef.current?.scrollTo({ top: boxRef.current.scrollHeight });
  }, [events]);

  return (
    <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
      <div style={{ fontSize: 11, letterSpacing: 1, color: 'var(--accent)', marginBottom: 4 }}>
        <span style={{ color: connected ? 'var(--green)' : 'var(--red)' }}>●</span> LIVE EVENT STREAM
        {!connected && <span style={{ color: 'var(--muted)' }}> — reconnecting…</span>}
      </div>
      <div ref={boxRef} style={{ flex: 1, overflowY: 'auto', background: '#0a0e18',
        border: '1px solid var(--border)', borderRadius: 6, padding: 8,
        fontFamily: 'var(--mono)', fontSize: 12 }}>
        {events.map((e, i) => (
          <div key={i} style={{ color: OUTCOME_COLOR[e.outcome] }}>
            {e.ts.slice(11, 19)} delivery {e.deliveryId.slice(-8)} epoch {e.epoch} → {e.outcome}
          </div>
        ))}
        {events.length === 0 && <span style={{ color: 'var(--muted)' }}>Waiting for events…</span>}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Mount in App**

```tsx
            ? <CampaignDetail id={selectedId} onError={setErrorBar}>
                <EventStream campaignId={selectedId} />
              </CampaignDetail>
```

(with `import { EventStream } from './EventStream';`)

- [ ] **Step 3: Manual acceptance (full spec checklist)**

With stack up, walk the spec's 5-point checklist: create / dispatch (stream scrolls green SUCCESS lines) / pause / resume (red REJECTED_STALE lines appear, epoch +1, drains to COMPLETED) / kill-restart backend (error bar + red dot, then auto-recovery).

- [ ] **Step 4: Typecheck, build, commit**

```bash
cd frontend && npx tsc --noEmit && npm run build
git add frontend/src
git commit -m "feat: live SSE event stream with connection indicator"
```

---

### Task 9: Demo shell — keynote engine

**Files:**
- Create (replacing old walkthrough files): `demo/index.html`, `demo/deck.css`, `demo/deck.js`
- Delete: `demo/app.js`, `demo/styles.css` (old walkthrough; keep `demo/snippets.js` until Task 10 extracts snippets, delete it there)

**Interfaces:**
- Consumes: nothing.
- Produces: slide engine — each `<section class="slide">` inside `<main id="deck">` is a slide; ←/→ / Space / click-right-edge navigate; `.slide.active` gets class `entered` on entry (CSS animations key off it); bottom progress bar segments auto-generated.

- [ ] **Step 1: index.html skeleton (slides filled in Tasks 10–11)**

```html
<!-- demo/index.html -->
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Campaign Dispatch Engine — NestJS · Temporal · Pulsar</title>
  <link rel="stylesheet" href="deck.css">
</head>
<body>
  <main id="deck">
    <section class="slide" data-title="Cover"><h1>Slide 1 placeholder</h1></section>
    <section class="slide" data-title="Problem"><h1>Slide 2 placeholder</h1></section>
    <section class="slide" data-title="Architecture"><h1>Slide 3 placeholder</h1></section>
    <section class="slide" data-title="Dispatch"><h1>Slide 4 placeholder</h1></section>
    <section class="slide" data-title="Epoch Fence"><h1>Slide 5 placeholder</h1></section>
    <section class="slide" data-title="Pause/Resume"><h1>Slide 6 placeholder</h1></section>
    <section class="slide" data-title="Reliability"><h1>Slide 7 placeholder</h1></section>
    <section class="slide" data-title="Run It"><h1>Slide 8 placeholder</h1></section>
  </main>
  <footer id="progress"></footer>
  <script src="deck.js"></script>
</body>
</html>
```

- [ ] **Step 2: deck.js (navigation engine)**

```javascript
// demo/deck.js — minimal keynote engine: keyboard/click nav + progress bar.
(() => {
  const slides = Array.from(document.querySelectorAll('.slide'));
  const progress = document.getElementById('progress');
  let index = 0;

  const segments = slides.map(() => {
    const s = document.createElement('div');
    s.className = 'seg';
    progress.appendChild(s);
    return s;
  });

  function show(i) {
    index = Math.max(0, Math.min(slides.length - 1, i));
    slides.forEach((sl, j) => {
      sl.classList.toggle('active', j === index);
      // 'entered' triggers per-slide CSS animations; removed when leaving so
      // re-visiting a slide replays its animation.
      if (j === index) requestAnimationFrame(() => sl.classList.add('entered'));
      else sl.classList.remove('entered');
      segments[j].classList.toggle('done', j <= index);
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowRight' || e.key === ' ' || e.key === 'PageDown') show(index + 1);
    if (e.key === 'ArrowLeft' || e.key === 'PageUp') show(index - 1);
    if (e.key === 'Home') show(0);
    if (e.key === 'End') show(slides.length - 1);
  });
  document.addEventListener('click', (e) => {
    if (e.target.closest('a, code, pre')) return; // don't hijack link/code clicks
    show(e.clientX > window.innerWidth / 2 ? index + 1 : index - 1);
  });

  show(0);
})();
```

- [ ] **Step 3: deck.css (frame + dark-tech base)**

```css
/* demo/deck.css — dark tech: deep blue-black, neon gradients, glow. */
:root {
  --bg: #0a0e1a; --text: #e5e7eb; --muted: #94a3b8;
  --cyan: #22d3ee; --indigo: #818cf8; --pink: #f472b6;
  --green: #4ade80; --red: #f87171;
  --mono: ui-monospace, SFMono-Regular, Menlo, monospace;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body { background: var(--bg); color: var(--text); overflow: hidden;
  font: 18px/1.6 -apple-system, "Segoe UI", Roboto, sans-serif; }

.slide { position: absolute; inset: 0 0 24px 0; padding: 8vh 10vw; display: none;
  flex-direction: column; justify-content: center; }
.slide.active { display: flex; }
.slide.entered .rise { opacity: 1; transform: none; }
.rise { opacity: 0; transform: translateY(18px); transition: opacity .6s, transform .6s; }
.rise.d1 { transition-delay: .15s; } .rise.d2 { transition-delay: .3s; }
.rise.d3 { transition-delay: .45s; } .rise.d4 { transition-delay: .6s; }

.kicker { font-size: 13px; letter-spacing: 3px; text-transform: uppercase; color: var(--cyan); }
h1 { font-size: clamp(36px, 6vw, 72px); line-height: 1.1; margin: .3em 0; font-weight: 800; }
.gradient { background: linear-gradient(90deg, var(--indigo), var(--cyan));
  -webkit-background-clip: text; background-clip: text; color: transparent; }
.sub { color: var(--muted); font-size: clamp(16px, 2vw, 22px); max-width: 60ch; }

.glow-card { background: rgba(255,255,255,.05); border: 1px solid rgba(129,140,248,.35);
  border-radius: 12px; padding: 18px 22px;
  box-shadow: 0 0 24px rgba(99,102,241,.15); }
.code-card { background: #0d1220; border: 1px solid #1f2937; border-radius: 10px;
  padding: 14px 18px; font-family: var(--mono); font-size: 14px; line-height: 1.5;
  overflow-x: auto; white-space: pre; color: #c9d1d9; }

#progress { position: fixed; bottom: 0; left: 0; right: 0; height: 24px;
  display: flex; gap: 6px; padding: 9px 10vw; }
.seg { flex: 1; border-radius: 3px; background: rgba(255,255,255,.12); transition: background .3s; }
.seg.done { background: linear-gradient(90deg, var(--indigo), var(--cyan)); }
```

- [ ] **Step 4: Verify + commit**

Open `demo/index.html` by double-click: 8 placeholder slides navigate with ←/→/Space/click, progress bar fills, no console errors.

```bash
git rm demo/app.js demo/styles.css
git add demo
git commit -m "feat: demo keynote shell — slide engine, progress bar, dark-tech theme"
```

---

### Task 10: Demo slides 1–4 (cover, problem, architecture, dispatch flow)

**Files:**
- Modify: `demo/index.html` (replace placeholder sections 1–4)
- Modify: `demo/deck.css` (append slide-specific styles)
- Reference then delete: `demo/snippets.js` (copy any needed snippet text into slide code-cards; `git rm` it in this task)

**Interfaces:**
- Consumes: `.rise`/`.glow-card`/`.code-card`/`.kicker`/`.gradient` classes from Task 9.
- Produces: slides 1–4 content. Exact copy below — implementer may refine visual polish but keep wording.

- [ ] **Step 1: Slide 1 — Cover**

```html
<section class="slide" data-title="Cover">
  <div class="kicker rise">Campaign Dispatch Engine</div>
  <h1 class="rise d1">Pause. Resume.<br><span class="gradient">Never dispatch twice.</span></h1>
  <p class="sub rise d2">A campaign delivery pipeline that survives pause, resume, retries and
    races — built on NestJS, Temporal and Pulsar.</p>
  <div class="rise d3" style="display:flex;gap:12px;margin-top:24px">
    <span class="glow-card" style="padding:8px 16px">NestJS</span>
    <span class="glow-card" style="padding:8px 16px">Temporal</span>
    <span class="glow-card" style="padding:8px 16px">Pulsar</span>
    <span class="glow-card" style="padding:8px 16px">MongoDB · Redis</span>
  </div>
</section>
```

- [ ] **Step 2: Slide 2 — The Problem**

```html
<section class="slide" data-title="Problem">
  <div class="kicker rise">01 — The Problem</div>
  <h1 class="rise d1">Pausing a campaign of<br><span class="gradient">a million messages</span> is hard.</h1>
  <div class="rise d2" style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:24px">
    <div class="glow-card"><b style="color:var(--red)">Duplicates</b><br>
      <span class="sub" style="font-size:16px">Resume re-sends messages that were already in flight.</span></div>
    <div class="glow-card"><b style="color:var(--red)">Races</b><br>
      <span class="sub" style="font-size:16px">Consumers and the pause signal fight over the same rows.</span></div>
    <div class="glow-card"><b style="color:var(--red)">Orphans</b><br>
      <span class="sub" style="font-size:16px">Rows stuck IN_PROGRESS forever when a worker dies mid-send.</span></div>
  </div>
</section>
```

- [ ] **Step 3: Slide 3 — Architecture (glowing node diagram)**

```html
<section class="slide" data-title="Architecture">
  <div class="kicker rise">02 — Architecture</div>
  <h1 class="rise d1">Five moving parts, <span class="gradient">one truth source.</span></h1>
  <div class="arch rise d2">
    <div class="node" style="--c:var(--cyan)">NestJS API<small>REST + SSE</small></div>
    <div class="arrow">→</div>
    <div class="node" style="--c:var(--indigo)">Temporal<small>workflows · activities</small></div>
    <div class="arrow">→</div>
    <div class="node" style="--c:var(--pink)">Pulsar<small>shared subscription</small></div>
    <div class="arrow">→</div>
    <div class="node" style="--c:var(--green)">Consumer<small>epoch fence + CAS</small></div>
  </div>
  <p class="sub rise d3" style="margin-top:20px">MongoDB rows are the source of truth; Redis holds the
    pause flag; every message carries its dispatch epoch.</p>
</section>
```

Append to `deck.css`:

```css
.arch { display: flex; align-items: center; gap: 14px; margin-top: 28px; flex-wrap: wrap; }
.node { border: 1px solid var(--c); border-radius: 12px; padding: 16px 20px; min-width: 150px;
  box-shadow: 0 0 22px color-mix(in srgb, var(--c) 30%, transparent); }
.node small { display: block; color: var(--muted); font-size: 13px; margin-top: 4px; }
.arrow { color: var(--muted); font-size: 24px; }
```

- [ ] **Step 4: Slide 4 — Dispatch flow (progressive light-up)**

```html
<section class="slide" data-title="Dispatch">
  <div class="kicker rise">03 — Dispatch Flow</div>
  <h1 class="rise d1">From one API call<br>to <span class="gradient">a drained backlog.</span></h1>
  <ol class="steps">
    <li class="rise d1"><b>POST /campaigns/:id/dispatch</b> starts the parent Temporal workflow.</li>
    <li class="rise d2"><b>Targeting activity</b> materialises PENDING delivery rows (idempotent).</li>
    <li class="rise d3"><b>Dispatcher workflow</b> claims batches PENDING → IN_PROGRESS, publishes to Pulsar.</li>
    <li class="rise d4"><b>Consumers</b> re-read the row, fence the epoch, CAS to SENDING, then finalize.</li>
  </ol>
  <div class="code-card rise d4" style="margin-top:18px">// every message carries its generation
{ deliveryId, campaignId, epoch }</div>
</section>
```

Append to `deck.css`:

```css
.steps { margin: 24px 0 0; padding-left: 24px; display: grid; gap: 12px; font-size: 19px; }
.steps b { color: var(--cyan); font-family: var(--mono); font-size: 16px; }
```

- [ ] **Step 5: Remove snippets.js, verify, commit**

```bash
git rm demo/snippets.js
```

Open `demo/index.html`: slides 1–4 render with staggered rise animations on entry (and replay when re-entered), no console errors.

```bash
git add demo
git commit -m "feat: demo slides 1-4 — cover, problem, architecture, dispatch flow"
```

---

### Task 11: Demo slides 5–8 (epoch fence animation, pause/resume, reliability, run it)

**Files:**
- Modify: `demo/index.html` (replace placeholder sections 5–8)
- Modify: `demo/deck.css` (append fence animation styles)

**Interfaces:**
- Consumes: shell classes from Task 9.
- Produces: final four slides; the epoch-fence slide is the flagship animation.

- [ ] **Step 1: Slide 5 — Epoch Fence (flagship animation)**

```html
<section class="slide" data-title="Epoch Fence">
  <div class="kicker rise">04 — The Epoch Fence</div>
  <h1 class="rise d1">One number makes<br><span class="gradient">resume safe.</span></h1>
  <div class="fence rise d2">
    <div class="msg stale">msg · epoch 1</div>
    <div class="wall">campaign<br>epoch = 2</div>
    <div class="msg fresh">msg · epoch 2</div>
  </div>
  <p class="sub rise d3">Resume bumps the campaign epoch atomically. In-flight messages from the old
    round carry the old number — the consumer compares and <b style="color:var(--red)">ack-skips</b> them
    without touching the row.</p>
  <div class="code-card rise d4">if (msg.epoch &lt; currentEpoch) return; // stale → REJECTED, row untouched</div>
</section>
```

Append to `deck.css`:

```css
.fence { display: flex; align-items: center; gap: 28px; margin: 26px 0 10px; }
.wall { border: 2px solid var(--indigo); border-radius: 12px; padding: 18px 22px; text-align: center;
  font-family: var(--mono); box-shadow: 0 0 30px rgba(129,140,248,.4); }
.msg { font-family: var(--mono); font-size: 15px; padding: 10px 14px; border-radius: 8px; }
.msg.stale { border: 1px solid var(--red); color: var(--red); }
.msg.fresh { border: 1px solid var(--green); color: var(--green); }
.slide.entered .msg.stale { animation: bounce-off 1.6s .8s both; }
.slide.entered .msg.fresh { animation: pass-through 1.6s 1.2s both; }
@keyframes bounce-off {
  0% { transform: translateX(0); opacity: 1; }
  45% { transform: translateX(46px); }
  70% { transform: translateX(-14px); }
  100% { transform: translateX(-8px); opacity: .45; }
}
@keyframes pass-through {
  0% { transform: translateX(0); }
  100% { transform: translateX(-120px); opacity: .9; }
}
```

- [ ] **Step 2: Slide 6 — Pause / Resume**

```html
<section class="slide" data-title="Pause/Resume">
  <div class="kicker rise">05 — Pause &amp; Resume</div>
  <h1 class="rise d1">Rewind, bump, <span class="gradient">relaunch.</span></h1>
  <ol class="steps">
    <li class="rise d1"><b>Pause</b> sets a Redis flag — the dispatcher stops claiming new batches.</li>
    <li class="rise d2"><b>Resume</b> clears the flag, atomically bumps the epoch (fences old messages).</li>
    <li class="rise d3"><b>Rewind</b> flips IN_PROGRESS rows back to PENDING — exactly the fenced ones.</li>
    <li class="rise d4"><b>Relaunch</b> starts a fresh workflow (epoch-scoped id, no collisions) and drains to COMPLETED.</li>
  </ol>
</section>
```

- [ ] **Step 3: Slide 7 — Reliability design**

```html
<section class="slide" data-title="Reliability">
  <div class="kicker rise">06 — Reliability</div>
  <h1 class="rise d1">Assume everything <span class="gradient">crashes.</span></h1>
  <div class="rise d2" style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:24px">
    <div class="glow-card"><b style="color:var(--cyan)">CAS transitions</b><br>
      <span class="sub" style="font-size:16px">Every status change is a compare-and-set; concurrent losers
      ack-skip instead of double-sending.</span></div>
    <div class="glow-card"><b style="color:var(--indigo)">Idempotent targeting</b><br>
      <span class="sub" style="font-size:16px">Re-running the parent workflow never duplicates the
      audience — rows are created once.</span></div>
    <div class="glow-card"><b style="color:var(--pink)">Reconciliation cron</b><br>
      <span class="sub" style="font-size:16px">A sweeper returns stuck rows to PENDING and completes
      campaigns whose backlog has drained.</span></div>
  </div>
</section>
```

- [ ] **Step 4: Slide 8 — Run it**

```html
<section class="slide" data-title="Run It">
  <div class="kicker rise">07 — Run It Yourself</div>
  <h1 class="rise d1">Three commands to a <span class="gradient">live console.</span></h1>
  <div class="code-card rise d2" style="margin-top:18px">git clone &lt;repo-url&gt; &amp;&amp; cd nestjs-temporal-pulsar-demo
docker compose up -d
cd backend &amp;&amp; npm i &amp;&amp; npm run build &amp;&amp; node dist/main.js
cd frontend &amp;&amp; npm i &amp;&amp; npm run dev   # console on :5173</div>
  <p class="sub rise d3" style="margin-top:18px">Create a campaign, dispatch it, pause mid-flight, resume —
    and watch stale messages bounce off the fence in the live event stream.</p>
</section>
```

- [ ] **Step 5: Full demo acceptance + commit**

Open `demo/index.html` offline (`file://`): all 8 slides navigable by keyboard and click, fence animation plays on slide 5 entry and replays on re-entry, no console errors.

```bash
git add demo
git commit -m "feat: demo slides 5-8 — epoch fence animation, pause/resume, reliability, run-it"
```

---

### Task 12: README + end-to-end walkthrough

**Files:**
- Modify: `README.md` (root)

**Interfaces:**
- Consumes: everything above.
- Produces: the definition-of-done path.

- [ ] **Step 1: Update README**

Rewrite the repo-structure and run sections to cover the three parts (keep existing architecture/design-talking-points content):

```markdown
## Repository layout

| Part | Path | What it is |
|---|---|---|
| Backend | `backend/` | NestJS + Temporal + Pulsar campaign dispatch engine |
| Console | `frontend/` | Vite + React runtime console (list, dispatch/pause/resume, live SSE event stream) |
| Demo | `demo/` | Static keynote-style presentation — open `demo/index.html`, navigate with ←/→ |

## Quick start

    docker compose up -d
    cd backend && npm i && npm run build && node dist/main.js
    # new terminal
    cd frontend && npm i && npm run dev    # console on http://localhost:5173
```

- [ ] **Step 2: Walk the definition-of-done end-to-end**

From a clean state (`docker compose down -v` first): follow the README verbatim — stack up, backend boots, console loads, create → dispatch → pause → resume → COMPLETED with red stale events visible, demo opens offline. Record any friction and fix the README (not the code) unless a real bug surfaces.

- [ ] **Step 3: Final checks + commit**

```bash
cd backend && npx tsc --noEmit && npx jest        # all green
cd ../frontend && npx tsc --noEmit && npm run build
git add README.md
git commit -m "docs: three-part layout and quick start"
docker compose down
```
