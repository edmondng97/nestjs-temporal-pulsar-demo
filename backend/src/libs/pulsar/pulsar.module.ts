import { Module } from '@nestjs/common';
import { PulsarClientProvider } from './pulsar-client.provider';
import { CampaignDeliveryProducer } from './campaign-delivery.producer';
import { CampaignDeliveryConsumer } from './campaign-delivery.consumer';
import { CampaignDeliveryModule } from '../../modules/campaign-delivery/campaign-delivery.module';
import { CampaignModule } from '../../modules/campaign/campaign.module';

@Module({
  imports: [CampaignDeliveryModule, CampaignModule],
  providers: [PulsarClientProvider, CampaignDeliveryProducer, CampaignDeliveryConsumer],
  exports: [PulsarClientProvider, CampaignDeliveryProducer, CampaignDeliveryConsumer],
})
export class PulsarModule {}
