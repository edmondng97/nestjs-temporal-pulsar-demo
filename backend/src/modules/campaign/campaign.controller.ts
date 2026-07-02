import { Controller, Get, Param, Post, Sse, MessageEvent } from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { Types } from 'mongoose';
import { CampaignService } from './campaign.service';
import { CampaignDeliveryService } from '../campaign-delivery/campaign-delivery.service';
import { RedisService } from '../../libs/redis/redis.service';
import { TemporalClientService } from '../../libs/temporal/temporal-client.service';
import { CampaignEventsService } from '../../libs/events/campaign-events.service';
import {
  EXECUTE_CAMPAIGN_WORKFLOW_TYPE,
  SCHEDULER_TASK_QUEUE,
  DISPATCHER_TASK_QUEUE,
  buildExecuteWorkflowId,
} from '../../libs/temporal/temporal.constants';
import { DELIVERY_STATUS } from '../../constants';

@Controller('campaigns')
export class CampaignController {
  constructor(
    private readonly campaignService: CampaignService,
    private readonly deliveryService: CampaignDeliveryService,
    private readonly redis: RedisService,
    private readonly temporal: TemporalClientService,
    private readonly events: CampaignEventsService,
  ) {}

  @Post()
  create() {
    return this.campaignService.create();
  }

  // Kick off the parent workflow. Dispatcher task queue is passed in so the
  // child runs on its own worker pool — mirrors the source service topology.
  @Post(':id/dispatch')
  async dispatch(@Param('id') id: string) {
    await this.temporal.client.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, {
      taskQueue: SCHEDULER_TASK_QUEUE,
      workflowId: buildExecuteWorkflowId(id),
      // Fresh campaign starts at epoch 0; child dispatcher id is epoch-scoped.
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE, epoch: 0 }],
    });
    return { started: true };
  }

  @Post(':id/pause')
  async pause(@Param('id') id: string) {
    await this.redis.setPaused(id, true);
    return { paused: true };
  }

  // Resume = clear pause flag + bump epoch + rewind. Bumping the epoch makes old
  // in-flight messages stale (fenced at the consumer). Rewinding IN_PROGRESS rows
  // back to PENDING re-queues exactly those rows whose old-epoch messages were
  // fenced, so the fresh dispatch round re-sends them under the new epoch and the
  // campaign can drain to completion.
  @Post(':id/resume')
  async resume(@Param('id') id: string) {
    const oid = new Types.ObjectId(id);
    await this.redis.setPaused(id, false);
    const epoch = await this.campaignService.bumpEpoch(oid);
    await this.deliveryService.rewindInProgressToPending(oid);
    await this.temporal.client.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, {
      taskQueue: SCHEDULER_TASK_QUEUE,
      workflowId: `${buildExecuteWorkflowId(id)}-resume-${epoch}`,
      // Pass the bumped epoch so the child dispatcher id is dispatch-<id>-<epoch>,
      // never colliding with a previous round's still-exiting dispatcher.
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE, epoch }],
    });
    return { resumed: true, epoch };
  }

  // SSE stream of per-delivery consumer outcomes. Declared before ':id' routes
  // so the literal path wins route matching.
  @Sse('events')
  sse(): Observable<MessageEvent> {
    return this.events.stream().pipe(map((e) => ({ data: e })));
  }

  @Get()
  async list() {
    const docs = await this.campaignService.findAll();
    return Promise.all(
      docs.map(async (d) => ({
        id: d._id.toString(),
        status: d.status,
        dispatchEpoch: d.dispatchEpoch,
        paused: await this.redis.isPaused(d._id.toString()),
        createdAt: (d as any).createdAt,
      })),
    );
  }

  @Get(':id')
  async get(@Param('id') id: string) {
    const oid = new Types.ObjectId(id);
    const campaign = await this.campaignService.findById(oid);
    const counts = {
      PENDING: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.PENDING),
      IN_PROGRESS: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.IN_PROGRESS),
      SENDING: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.SENDING),
      SUCCESS: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.SUCCESS),
      FAILED: await this.deliveryService.countByStatus(oid, DELIVERY_STATUS.FAILED),
    };
    return { campaign, counts, paused: await this.redis.isPaused(id) };
  }
}
