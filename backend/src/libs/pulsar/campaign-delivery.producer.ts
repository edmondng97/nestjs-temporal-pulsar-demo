import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Client, Producer } from 'pulsar-client';
import {
  CAMPAIGN_DELIVERY_PULSAR_CLIENT,
  CAMPAIGN_DELIVERY_TOPIC,
  CampaignDeliveryMessage,
} from './pulsar.constants';

@Injectable()
export class CampaignDeliveryProducer implements OnModuleInit {
  private producer: Producer;

  constructor(@Inject(CAMPAIGN_DELIVERY_PULSAR_CLIENT) private readonly client: Client) {}

  async onModuleInit(): Promise<void> {
    this.producer = await this.client.createProducer({ topic: CAMPAIGN_DELIVERY_TOPIC });
  }

  async send(msg: CampaignDeliveryMessage): Promise<void> {
    await this.producer.send({ data: Buffer.from(JSON.stringify(msg)) });
  }
}
