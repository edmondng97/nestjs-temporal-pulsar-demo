import type { ReactNode } from 'react';
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
