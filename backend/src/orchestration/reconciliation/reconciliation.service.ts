import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { CampaignService } from '../../modules/campaign/campaign.service';
import { CampaignDeliveryService } from '../../modules/campaign-delivery/campaign-delivery.service';
import { DELIVERY_STATUS } from '../../constants';

@Injectable()
export class ReconciliationService {
  constructor(
    private readonly campaignService: CampaignService,
    private readonly deliveryService: CampaignDeliveryService,
  ) {}

  @Cron(CronExpression.EVERY_10_SECONDS)
  async reconcile(): Promise<void> {
    await this.reconcileOnce();
  }

  // A campaign is COMPLETED once no delivery is still PENDING/IN_PROGRESS/SENDING.
  // This is the safety net that owns campaign completion — not the dispatcher.
  async reconcileOnce(): Promise<void> {
    const campaigns = await this.campaignService.listInProgress();
    for (const c of campaigns) {
      const nonTerminal =
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.PENDING)) +
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.IN_PROGRESS)) +
        (await this.deliveryService.countByStatus(c._id, DELIVERY_STATUS.SENDING));
      if (nonTerminal === 0) {
        await this.campaignService.markCompleted(c._id);
      }
    }
  }
}
