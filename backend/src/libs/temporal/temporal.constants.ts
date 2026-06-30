export const SCHEDULER_TASK_QUEUE = 'campaign-scheduler';
export const DISPATCHER_TASK_QUEUE = 'campaign-dispatcher';

export const EXECUTE_CAMPAIGN_WORKFLOW_TYPE = 'executeCampaignWorkflow';
export const CAMPAIGN_DISPATCHER_WORKFLOW_TYPE = 'campaignDispatcherWorkflow';

export const buildExecuteWorkflowId = (campaignId: string) => `execute-${campaignId}`;
export const buildDispatcherWorkflowId = (campaignId: string) => `dispatch-${campaignId}`;
