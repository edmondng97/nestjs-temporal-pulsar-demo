import { TOKENS } from '../../constants';

export const CAMPAIGN_DELIVERY_TOPIC = 'persistent://public/default/campaign-delivery';
export const CAMPAIGN_DELIVERY_SUBSCRIPTION = 'campaign-delivery-sub';
export const CAMPAIGN_DELIVERY_PULSAR_CLIENT = TOKENS.PULSAR_CLIENT;

export interface CampaignDeliveryMessage {
  deliveryId: string;
  campaignId: string;
  epoch: number;
}
