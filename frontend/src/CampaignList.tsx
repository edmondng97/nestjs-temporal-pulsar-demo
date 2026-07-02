import type { CampaignSummary } from './api';
import { createCampaign } from './api';

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
