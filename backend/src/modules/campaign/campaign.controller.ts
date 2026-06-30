import { Controller, Get, Param, Post } from '@nestjs/common';
import { Types } from 'mongoose';
import { CampaignService } from './campaign.service';
import { CampaignDeliveryService } from '../campaign-delivery/campaign-delivery.service';
import { RedisService } from '../../libs/redis/redis.service';
import { TemporalClientService } from '../../libs/temporal/temporal-client.service';
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
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE }],
    });
    return { started: true };
  }

  @Post(':id/pause')
  async pause(@Param('id') id: string) {
    await this.redis.setPaused(id, true);
    return { paused: true };
  }

  // Resume = clear pause flag + bump epoch. Old in-flight messages become stale
  // and are fenced at the consumer; un-sent PENDING rows get re-dispatched.
  @Post(':id/resume')
  async resume(@Param('id') id: string) {
    await this.redis.setPaused(id, false);
    const epoch = await this.campaignService.bumpEpoch(new Types.ObjectId(id));
    await this.temporal.client.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, {
      taskQueue: SCHEDULER_TASK_QUEUE,
      workflowId: `${buildExecuteWorkflowId(id)}-resume-${epoch}`,
      args: [{ campaignId: id, dispatcherTaskQueue: DISPATCHER_TASK_QUEUE }],
    });
    return { resumed: true, epoch };
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
    return { campaign, counts };
  }
}
