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
