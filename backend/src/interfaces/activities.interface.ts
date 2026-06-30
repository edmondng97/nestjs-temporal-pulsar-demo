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
}

export interface CampaignDispatcherWorkflow_Input_Interface {
  campaignId: string;
  epoch: number;
  dispatchChunk: number;
  dispatcherConcurrency: number;
}
