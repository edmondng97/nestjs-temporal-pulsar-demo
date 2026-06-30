import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Types } from 'mongoose';
import { DELIVERY_STATUS } from '../constants';

export type CampaignDeliveryDocument = HydratedDocument<CampaignDelivery>;

@Schema({ collection: 'campaign_deliveries', timestamps: true })
export class CampaignDelivery {
  @Prop({ type: Types.ObjectId, required: true, index: true })
  campaignId: Types.ObjectId;

  @Prop({ required: true, default: DELIVERY_STATUS.PENDING, index: true })
  status: string;

  @Prop()
  completedAt?: Date;

  @Prop()
  errorMessage?: string;
}

export const CampaignDeliverySchema = SchemaFactory.createForClass(CampaignDelivery);
