import { Types } from 'mongoose';
import { CampaignDeliveryDocument } from '../schemas/campaign-delivery.schema';

// Port consumed by the dispatch activity. Keeps the activity decoupled from the
// concrete Mongoose service so it depends on behaviour, not implementation.
export interface ICampaignDeliveryDispatchPort {
  findPendingPage(campaignId: Types.ObjectId, limit: number): Promise<CampaignDeliveryDocument[]>;
  markInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }>;
}
