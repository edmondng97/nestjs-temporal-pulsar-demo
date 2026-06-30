import { Injectable } from '@nestjs/common';
import { Types } from 'mongoose';
import { CampaignDeliveryService } from '../../campaign-delivery/campaign-delivery.service';
import { Targeting_Activity_Interface } from '../../../interfaces/activities.interface';

@Injectable()
export class TargetingActivity implements Targeting_Activity_Interface {
  constructor(private readonly deliveryService: CampaignDeliveryService) {}

  // Business targeting stripped: just materialise a fixed batch of PENDING rows.
  async targeting(input: { campaignId: string }): Promise<void> {
    await this.deliveryService.createMany(new Types.ObjectId(input.campaignId), 1000);
  }
}
