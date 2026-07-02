import { useEffect, useRef, useState } from 'react';
import { API_URL } from './api';
import type { DeliveryEvent } from './api';

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
