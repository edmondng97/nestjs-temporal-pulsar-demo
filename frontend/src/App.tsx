import { useState } from 'react';
import './theme.css';
import { listCampaigns } from './api';
import { usePolling } from './usePolling';
import { CampaignList } from './CampaignList';
import { CampaignDetail } from './CampaignDetail';

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
            ? <CampaignDetail id={selectedId} onError={setErrorBar} />
            : <p style={{ color: 'var(--muted)' }}>Select or create a campaign.</p>}
        </main>
      </div>
    </div>
  );
}
