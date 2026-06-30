export interface Targeting_Activity_Interface {
  targeting(input: { campaignId: string }): Promise<void>;
}

export interface DispatchPlayers_Activity_Interface {
  dispatchPlayers(input: {
    campaignId: string;
    epoch: number;
    dispatchChunk: number;
    dispatcherConcurrency: number;
  }): Promise<void>;
}

export interface ExecuteCampaignWorkflow_Input_Interface {
  campaignId: string;
  dispatcherTaskQueue: string;
  // Dispatch generation. Used to build a per-round child workflowId so a
  // resume's dispatcher never collides with a still-exiting previous one.
  epoch: number;
}

export interface CampaignDispatcherWorkflow_Input_Interface {
  campaignId: string;
  epoch: number;
  dispatchChunk: number;
  dispatcherConcurrency: number;
}
