import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { CampaignDelivery, CampaignDeliverySchema } from '../../schemas/campaign-delivery.schema';
import { CampaignDeliveryService } from './campaign-delivery.service';
import { TOKENS } from '../../constants';

@Module({
  imports: [MongooseModule.forFeature([{ name: CampaignDelivery.name, schema: CampaignDeliverySchema }])],
  providers: [
    CampaignDeliveryService,
    { provide: TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT, useExisting: CampaignDeliveryService },
  ],
  exports: [CampaignDeliveryService, TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT],
})
export class CampaignDeliveryModule {}
