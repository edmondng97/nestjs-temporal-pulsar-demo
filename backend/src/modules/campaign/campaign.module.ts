import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Campaign, CampaignSchema } from '../../schemas/campaign.schema';
import { CampaignService } from './campaign.service';
import { CampaignController } from './campaign.controller';
import { CampaignDeliveryModule } from '../campaign-delivery/campaign-delivery.module';
import { RedisService } from '../../libs/redis/redis.service';
import { TemporalClientService } from '../../libs/temporal/temporal-client.service';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: Campaign.name, schema: CampaignSchema }]),
    CampaignDeliveryModule,
  ],
  controllers: [CampaignController],
  providers: [CampaignService, RedisService, TemporalClientService],
  exports: [CampaignService, RedisService, TemporalClientService],
})
export class CampaignModule {}
