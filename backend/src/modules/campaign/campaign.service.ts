import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import { Campaign, CampaignDocument } from '../../schemas/campaign.schema';
import { CAMPAIGN_STATUS } from '../../constants';

@Injectable()
export class CampaignService {
  constructor(
    @InjectModel(Campaign.name) private readonly model: Model<CampaignDocument>,
  ) {}

  async create(): Promise<{ id: string }> {
    const doc = await this.model.create({ status: CAMPAIGN_STATUS.PENDING, dispatchEpoch: 0 });
    return { id: doc._id.toString() };
  }

  findById(id: Types.ObjectId): Promise<CampaignDocument | null> {
    return this.model.findById(id).exec();
  }

  // CAS PENDING -> IN_PROGRESS. Idempotent dispatch claim.
  async claimInProgressIfPending(id: Types.ObjectId): Promise<{ modifiedCount: number }> {
    const r = await this.model.updateOne(
      { _id: id, status: CAMPAIGN_STATUS.PENDING },
      { $set: { status: CAMPAIGN_STATUS.IN_PROGRESS, lastExecutionDate: new Date() } },
    );
    return { modifiedCount: r.modifiedCount };
  }

  async getDispatchEpoch(id: Types.ObjectId): Promise<number> {
    const doc = await this.model.findById(id, { dispatchEpoch: 1 }).exec();
    return doc?.dispatchEpoch ?? 0;
  }

  // Atomic increment so resume fences in-flight messages without a read-then-write race.
  async bumpEpoch(id: Types.ObjectId): Promise<number> {
    const doc = await this.model.findByIdAndUpdate(
      id,
      { $inc: { dispatchEpoch: 1 } },
      { new: true },
    ).exec();
    return doc?.dispatchEpoch ?? 0;
  }

  async markCompleted(id: Types.ObjectId): Promise<void> {
    await this.model.updateOne(
      { _id: id, status: CAMPAIGN_STATUS.IN_PROGRESS },
      { $set: { status: CAMPAIGN_STATUS.COMPLETED } },
    );
  }

  async listInProgress(): Promise<CampaignDocument[]> {
    return this.model.find({ status: CAMPAIGN_STATUS.IN_PROGRESS }).exec();
  }
}
