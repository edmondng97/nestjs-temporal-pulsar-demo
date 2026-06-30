import { INestApplicationContext } from '@nestjs/common';
import { Worker, NativeConnection } from '@temporalio/worker';
import { TargetingActivity } from '../../modules/campaign-temporal-scheduler/activities/targeting.activity';
import { DispatchPlayersActivity } from '../../modules/campaign-delivery-temporal/activities/dispatch-players.activity';
import { SCHEDULER_TASK_QUEUE, DISPATCHER_TASK_QUEUE } from './temporal.constants';

// Two workers on two task queues, mirroring source topology (scheduler vs dispatcher).
// Activities are pulled from the Nest container so they keep their injected deps.
export async function startTemporalWorkers(app: INestApplicationContext): Promise<void> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });

  const targeting = app.get(TargetingActivity);
  const dispatch = app.get(DispatchPlayersActivity);

  const schedulerWorker = await Worker.create({
    connection,
    namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    taskQueue: SCHEDULER_TASK_QUEUE,
    workflowsPath: require.resolve('../../modules/campaign-temporal-scheduler/workflows/execute-campaign.workflow'),
    activities: { targeting: targeting.targeting.bind(targeting) },
  });

  const dispatcherWorker = await Worker.create({
    connection,
    namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    taskQueue: DISPATCHER_TASK_QUEUE,
    workflowsPath: require.resolve('../../modules/campaign-delivery-temporal/workflows/campaign-dispatcher.workflow'),
    activities: { dispatchPlayers: dispatch.dispatchPlayers.bind(dispatch) },
  });

  void schedulerWorker.run();
  void dispatcherWorker.run();
}
