# Three-Part Project Split — Design Spec

Date: 2026-07-02
Status: Approved by user (brainstorming session)

## Goal

Split the repo into three parts within a single repository:

1. **backend/** — existing NestJS + Temporal + Pulsar campaign dispatch engine (minimal changes)
2. **frontend/** — new Vite + React runtime console that connects to the live backend API
3. **demo/** — static, presentation-grade keynote-style narrative page (dark tech visual style)

Users clone one repo, run `docker compose up` + backend + frontend to test the system interactively; the demo page is a standalone, offline-capable presentation aid.

## Directory Structure

```
nestjs-temporal-pulsar-demo/
├── backend/    # existing NestJS + Temporal + Pulsar (mostly untouched)
├── frontend/   # NEW: Vite + React + TypeScript console
├── demo/       # rebuilt static keynote page (content migrated from old frontend/)
└── docker-compose.yml
```

The current static walkthrough in `frontend/` (index.html, app.js, snippets.js, styles.css) is migrated into `demo/` as source material and rebuilt; `frontend/` is repurposed for the React console.

## Part 1 — Backend Changes (only two)

1. **SSE endpoint**: `GET /campaigns/events` using NestJS native `@Sse()`. The Pulsar consumer emits an event for every processed delivery, including stale-epoch rejections. Event payload includes campaign id, delivery/player id, resulting status (SENT / FAILED / REJECTED-stale), epoch, timestamp.
2. **CORS**: enable for the frontend dev server origin.

No other backend logic changes. Existing 7 tests must stay green.

## Part 2 — Frontend Console (`frontend/`)

**Stack**: Vite + React + TypeScript. No UI component library (hand-written dark-theme CSS matching the demo's visual language). No state management library (hooks only).

**Layout** (list + detail + embedded event stream):

- **Top bar**: title + "New Campaign" button → modal form with fields required by the backend create API (name, player count, etc.).
- **Left panel**: campaign list; each row shows name + status dot (PENDING grey / IN_PROGRESS green / PAUSED yellow / COMPLETED blue). Click to select.
- **Right panel** (selected campaign):
  - Status badge + current epoch
  - Progress bar (terminal-state deliveries / total) + per-status counts (PENDING / IN_PROGRESS / SENT / FAILED)
  - Action buttons: Dispatch / Pause / Resume — enabled/disabled by current status
  - Embedded **live event stream**: monospace scrolling log fed by SSE; SENT in green, stale-epoch REJECTED in red; filtered to the selected campaign

**Data flow**:

- Campaign list + detail: polling every 2s
- Event stream: native `EventSource` subscribing to the SSE endpoint
- Backend URL via `VITE_API_URL`, default `http://localhost:3000`

**Error handling**: toast-style error bar on API failure; SSE auto-reconnect (native EventSource behavior) with a connection status dot above the stream.

## Part 3 — Demo Page (`demo/`)

**Form**: pure static HTML/CSS/JS, zero build, works by opening `index.html` directly (GitHub Pages compatible). Full-screen paginated keynote: keyboard ←/→ or click to navigate, bottom progress bar.

**Visual style**: dark tech — deep blue-black background, neon gradient highlights, glowing nodes (user-selected style A from visual mockups).

**Narrative (8 slides)**:

1. **Cover** — project name + tagline ("Pause. Resume. Never dispatch twice.") + stack badges
2. **The Problem** — why pause/resume of million-message campaigns is hard (duplicates, races, orphans)
3. **Architecture Overview** — glowing node diagram (NestJS / Temporal / Pulsar / Redis / Postgres) with data-flow animation
4. **Dispatch Flow** — step-by-step animation from API call to Pulsar consumption (nodes light up progressively)
5. **Epoch Fence** — flagship slide: animation showing a stale (old-epoch) message being rejected
6. **Pause / Resume** — state machine + rewind (IN_PROGRESS → PENDING) demonstration
7. **Reliability Design** — three cards: CAS status transitions, idempotent targeting, reconciliation cron
8. **Closing** — how to run it (clone → docker compose → open console) + repo link

Text is speaker-note minimal (big title + few supporting lines). Selected code snippets from the existing `snippets.js` appear as polished code cards on flow slides.

**Animation rules**: CSS transitions/keyframes only, triggered on slide entry; no animation libraries.

## Testing & Acceptance

**Backend (SSE addition)**:

- Unit test for the event publishing path (consumer → SSE emitter); existing 7 tests stay green
- Manual e2e: `curl -N` on the SSE endpoint during a dispatch shows live events, including stale REJECTED events during pause/resume

**Frontend console** — manual acceptance checklist (no component-level automated tests; demo-project cost/benefit):

1. Create campaign → appears in list
2. Dispatch → status IN_PROGRESS, progress bar advances, event stream scrolls
3. Pause → status PAUSED, buttons toggle
4. Resume → epoch +1, rewind applies, red stale REJECTED events appear, campaign reaches COMPLETED
5. Kill backend → error bar + SSE disconnect indicator; restart → auto-recovery

Plus: `tsc --noEmit` clean, `npm run build` produces a working bundle.

**Demo page** — manual acceptance: all 8 slides navigable by keyboard/click, per-slide animations trigger on entry, no console errors, opens offline by double-clicking `index.html`.

**Definition of done**: the README path (clone → `docker compose up` → start backend → start frontend → open demo) can be walked end-to-end from scratch.

## Out of Scope

- Infrastructure health panel (Temporal/Pulsar/Redis/Postgres connection status)
- WebSocket transport (SSE chosen)
- Frontend automated component tests
- Any backend refactoring beyond the SSE endpoint + CORS
