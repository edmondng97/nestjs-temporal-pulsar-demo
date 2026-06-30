export const SCHEDULER_TASK_QUEUE = 'campaign-scheduler';
export const DISPATCHER_TASK_QUEUE = 'campaign-dispatcher';

export const EXECUTE_CAMPAIGN_WORKFLOW_TYPE = 'executeCampaignWorkflow';
export const CAMPAIGN_DISPATCHER_WORKFLOW_TYPE = 'campaignDispatcherWorkflow';

export const buildExecuteWorkflowId = (campaignId: string) => `execute-${campaignId}`;
// Epoch in the id makes each dispatch round a distinct child run. Without it, a
// fast pause→resume can collide: the new parent's startChild reuses the id of a
// previous dispatcher that has not yet exited → WorkflowExecutionAlreadyStarted.
export const buildDispatcherWorkflowId = (campaignId: string, epoch: number) =>
  `dispatch-${campaignId}-${epoch}`;
