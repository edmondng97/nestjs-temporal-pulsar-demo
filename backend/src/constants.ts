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
