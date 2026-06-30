import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types, FilterQuery } from 'mongoose';
import { CampaignDelivery, CampaignDeliveryDocument } from '../../schemas/campaign-delivery.schema';
import { DELIVERY_STATUS } from '../../constants';
import { ICampaignDeliveryDispatchPort } from '../../interfaces/campaign-delivery.port';

@Injectable()
export class CampaignDeliveryService implements ICampaignDeliveryDispatchPort {
  constructor(
    @InjectModel(CampaignDelivery.name) private readonly model: Model<CampaignDeliveryDocument>,
  ) {}

  async createMany(campaignId: Types.ObjectId, count: number): Promise<void> {
    const docs = Array.from({ length: count }, () => ({
      campaignId,
      status: DELIVERY_STATUS.PENDING,
    }));
    await this.model.insertMany(docs);
  }

  findOne(filter: FilterQuery<CampaignDeliveryDocument>): Promise<CampaignDeliveryDocument | null> {
    return this.model.findOne(filter).exec();
  }

  findPendingPage(campaignId: Types.ObjectId, limit: number): Promise<CampaignDeliveryDocument[]> {
    return this.model
      .find({ campaignId, status: DELIVERY_STATUS.PENDING })
      .limit(limit)
      .exec();
  }

  // CAS PENDING -> IN_PROGRESS. modifiedCount=1 means this caller claimed the row.
  async markInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: DELIVERY_STATUS.PENDING },
      { $set: { status: DELIVERY_STATUS.IN_PROGRESS } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async markSendingIfInProgress(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: DELIVERY_STATUS.IN_PROGRESS },
      { $set: { status: DELIVERY_STATUS.SENDING } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async markTerminal(input: {
    deliveryId: Types.ObjectId;
    status: string;
    completedAt: Date;
    errorMessage?: string;
  }): Promise<void> {
    await this.model.updateOne(
      { _id: input.deliveryId },
      {
        $set: {
          status: input.status,
          completedAt: input.completedAt,
          ...(input.errorMessage ? { errorMessage: input.errorMessage } : {}),
        },
      },
    );
  }

  async countByStatus(campaignId: Types.ObjectId, status: string): Promise<number> {
    return this.model.countDocuments({ campaignId, status });
  }
}
