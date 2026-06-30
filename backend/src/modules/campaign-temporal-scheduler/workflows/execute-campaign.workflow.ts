import { proxyActivities, startChild, ParentClosePolicy } from '../../../libs/temporal/workflow';
import {
  CAMPAIGN_DISPATCHER_WORKFLOW_TYPE,
  buildDispatcherWorkflowId,
} from '../../../libs/temporal/temporal.constants';
import type {
  Targeting_Activity_Interface,
  ExecuteCampaignWorkflow_Input_Interface,
} from '../../../interfaces/activities.interface';

const { targeting } = proxyActivities<Targeting_Activity_Interface>({
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, initialInterval: '5s', backoffCoefficient: 2 },
});

// Parent workflow: build the audience, then hand off to the dispatcher child.
export async function executeCampaignWorkflow(
  input: ExecuteCampaignWorkflow_Input_Interface,
): Promise<void> {
  const { campaignId, dispatcherTaskQueue } = input;

  await targeting({ campaignId });

  await startChild(CAMPAIGN_DISPATCHER_WORKFLOW_TYPE, {
    workflowId: buildDispatcherWorkflowId(campaignId),
    taskQueue: dispatcherTaskQueue,
    // ABANDON: parent completes immediately; without it the default TERMINATE
    // policy would kill the still-running dispatcher when the parent closes.
    parentClosePolicy: ParentClosePolicy.ABANDON,
    args: [{ campaignId, epoch: 0, dispatchChunk: 500, dispatcherConcurrency: 100 }],
  });
}
