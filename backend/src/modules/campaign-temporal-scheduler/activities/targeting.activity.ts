import { Injectable } from '@nestjs/common';
import { Types } from 'mongoose';
import { CampaignDeliveryService } from '../../campaign-delivery/campaign-delivery.service';
import { Targeting_Activity_Interface } from '../../../interfaces/activities.interface';

@Injectable()
export class TargetingActivity implements Targeting_Activity_Interface {
  constructor(private readonly deliveryService: CampaignDeliveryService) {}

  // Business targeting stripped: just materialise a fixed batch of PENDING rows.
  // Idempotent: a resume re-runs the parent workflow, so skip if the audience
  // already exists to avoid duplicating the delivery set.
  async targeting(input: { campaignId: string }): Promise<void> {
    const campaignId = new Types.ObjectId(input.campaignId);
    if (await this.deliveryService.existsForCampaign(campaignId)) return;
    await this.deliveryService.createMany(campaignId, 1000);
  }
}
