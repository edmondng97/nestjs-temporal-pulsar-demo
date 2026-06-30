import { Module } from '@nestjs/common';
import { ReconciliationService } from './reconciliation.service';
import { CampaignModule } from '../../modules/campaign/campaign.module';
import { CampaignDeliveryModule } from '../../modules/campaign-delivery/campaign-delivery.module';

@Module({
  imports: [CampaignModule, CampaignDeliveryModule],
  providers: [ReconciliationService],
})
export class ReconciliationModule {}
