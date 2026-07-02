import { Module } from '@nestjs/common';
import { CampaignEventsService } from './campaign-events.service';

// Single shared instance: both the Pulsar consumer (producer side) and the
// campaign controller (SSE side) must see the SAME Subject.
@Module({
  providers: [CampaignEventsService],
  exports: [CampaignEventsService],
})
export class CampaignEventsModule {}
