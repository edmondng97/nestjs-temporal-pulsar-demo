import { Inject, Injectable } from '@nestjs/common';
import { heartbeat } from '@temporalio/activity';
import { Types } from 'mongoose';
import { TOKENS, CAMPAIGN_STATUS } from '../../../constants';
import { ICampaignDeliveryDispatchPort } from '../../../interfaces/campaign-delivery.port';
import { DispatchPlayers_Activity_Interface } from '../../../interfaces/activities.interface';
import { CampaignDeliveryProducer } from '../../../libs/pulsar/campaign-delivery.producer';
import { RedisService } from '../../../libs/redis/redis.service';
import { CampaignService } from '../../campaign/campaign.service';

@Injectable()
export class DispatchPlayersActivity implements DispatchPlayers_Activity_Interface {
  constructor(
    @Inject(TOKENS.CAMPAIGN_DELIVERY_DISPATCH_PORT)
    private readonly dispatchPort: ICampaignDeliveryDispatchPort,
    private readonly producer: CampaignDeliveryProducer,
    private readonly redis: RedisService,
    private readonly campaignService: CampaignService,
  ) {}

  async dispatchPlayers(input: {
    campaignId: string;
    epoch: number;
    dispatchChunk: number;
    dispatcherConcurrency: number;
  }): Promise<void> {
    const campaignId = new Types.ObjectId(input.campaignId);
    const { dispatchChunk, dispatcherConcurrency } = input;

    // Read the authoritative epoch once at dispatch start; stamp it on every message.
    const epoch = await this.campaignService.getDispatchEpoch(campaignId);

    // Claim campaign PENDING -> IN_PROGRESS. Idempotent: re-runs get modifiedCount 0.
    await this.campaignService.claimInProgressIfPending(campaignId);

    let processed = 0;
    for (;;) {
      if (await this.redis.isPaused(input.campaignId)) break; // early stop; epoch covers correctness

      const page = await this.dispatchPort.findPendingPage(campaignId, dispatchChunk);
      if (page.length === 0) break;

      await this.mapWithConcurrency(page, dispatcherConcurrency, async (delivery) => {
        // Mark-first: CAS PENDING->IN_PROGRESS, send ONLY on win. A row marked but
        // not sent (crash) becomes a ghost reaped by reconciliation; a row not yet
        // marked stays PENDING for the next dispatch round.
        const cas = await this.dispatchPort.markInProgressIfPending(delivery._id);
        if (cas.modifiedCount !== 1) return;
        await this.producer.send({
          deliveryId: delivery._id.toString(),
          campaignId: input.campaignId,
          epoch,
        });
      });

      processed += page.length;
      heartbeat({ processed });
    }
  }

  // Bounded-concurrency map: N workers drain a shared index.
  private async mapWithConcurrency<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
    let index = 0;
    const worker = async () => {
      while (index < items.length) {
        const i = index++;
        await fn(items[i]);
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  }
}
