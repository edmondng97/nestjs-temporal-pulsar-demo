import { Inject, Injectable } from '@nestjs/common';
import { Client, Consumer } from 'pulsar-client';
import { Types } from 'mongoose';
import {
  CAMPAIGN_DELIVERY_PULSAR_CLIENT,
  CAMPAIGN_DELIVERY_TOPIC,
  CAMPAIGN_DELIVERY_SUBSCRIPTION,
  CampaignDeliveryMessage,
} from './pulsar.constants';
import { CampaignDeliveryService } from '../../modules/campaign-delivery/campaign-delivery.service';
import { CampaignService } from '../../modules/campaign/campaign.service';
import { DELIVERY_STATUS } from '../../constants';

@Injectable()
export class CampaignDeliveryConsumer {
  private consumer: Consumer;

  constructor(
    @Inject(CAMPAIGN_DELIVERY_PULSAR_CLIENT) private readonly client: Client,
    private readonly deliveryService: CampaignDeliveryService,
    private readonly campaignService: CampaignService,
  ) {}

  // Shared subscription → many consumers can drain the same backlog in parallel.
  async start(): Promise<void> {
    this.consumer = await this.client.subscribe({
      topic: CAMPAIGN_DELIVERY_TOPIC,
      subscription: CAMPAIGN_DELIVERY_SUBSCRIPTION,
      subscriptionType: 'Shared',
      receiverQueueSize: 100,
      ackTimeoutMs: 5 * 60 * 1000,
    });
    void this.receiveLoop();
  }

  private async receiveLoop(): Promise<void> {
    // Run forever; each iteration handles one message then acks/nacks.
    for (;;) {
      const msg = await this.consumer.receive();
      const data = JSON.parse(msg.getData().toString()) as CampaignDeliveryMessage;
      try {
        await this.handle(data);
        await this.consumer.acknowledge(msg);
      } catch (err) {
        // Do NOT ack on unknown error → Pulsar redelivers after ackTimeout.
        // A blanket ack here would silently drop deliveries forever.
        this.consumer.negativeAcknowledge(msg);
      }
    }
  }

  private async handle(data: CampaignDeliveryMessage): Promise<void> {
    const deliveryId = new Types.ObjectId(data.deliveryId);

    // Step 0: re-read. Truth source is the row, not the message. Must be IN_PROGRESS.
    const delivery = await this.deliveryService.findOne({ _id: deliveryId });
    if (!delivery || delivery.status !== DELIVERY_STATUS.IN_PROGRESS) return; // ack-skip

    const campaignId = delivery.campaignId as Types.ObjectId;

    // Step 0.5: epoch fence. A stale (pre-resume) message must not touch the row.
    const currentEpoch = await this.campaignService.getDispatchEpoch(campaignId);
    if (data.epoch < currentEpoch) return; // ack-skip, row untouched

    // Step 1: CAS IN_PROGRESS → SENDING. Concurrent loser → ack-skip.
    const cas = await this.deliveryService.markSendingIfInProgress(deliveryId);
    if (cas.modifiedCount !== 1) return;

    // Step 2: deliver stub — business logic stripped. Random success/failure.
    const ok = await this.deliverStub();

    // Step 3: finalize terminal status.
    await this.deliveryService.markTerminal({
      deliveryId,
      status: ok ? DELIVERY_STATUS.SUCCESS : DELIVERY_STATUS.FAILED,
      completedAt: new Date(),
      ...(ok ? {} : { errorMessage: 'stub_random_failure' }),
    });
  }

  // Placeholder for the real channels (SMS/Email/Mail). Kept intentionally trivial.
  private async deliverStub(): Promise<boolean> {
    await new Promise((r) => setTimeout(r, 50));
    return Math.random() > 0.1;
  }
}
