import { proxyActivities } from '../../../libs/temporal/workflow';
import type {
  DispatchPlayers_Activity_Interface,
  CampaignDispatcherWorkflow_Input_Interface,
} from '../../../interfaces/activities.interface';

const { dispatchPlayers } = proxyActivities<DispatchPlayers_Activity_Interface>({
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, initialInterval: '1s', backoffCoefficient: 2, maximumInterval: '1m' },
});

// Dispatcher's sole job: fan out delivery messages, then exit.
export async function campaignDispatcherWorkflow(
  input: CampaignDispatcherWorkflow_Input_Interface,
): Promise<void> {
  await dispatchPlayers(input);
}
