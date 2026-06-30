import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import { CAMPAIGN_STATUS } from '../constants';

export type CampaignDocument = HydratedDocument<Campaign>;

@Schema({ collection: 'campaigns', timestamps: true })
export class Campaign {
  @Prop({ required: true, default: CAMPAIGN_STATUS.PENDING })
  status: string;

  // Monotonic dispatch generation. Bumped on resume so in-flight (pre-pause)
  // messages become stale and are fenced out at the consumer.
  @Prop({ required: true, default: 0 })
  dispatchEpoch: number;

  @Prop()
  lastExecutionDate?: Date;
}

export const CampaignSchema = SchemaFactory.createForClass(Campaign);
