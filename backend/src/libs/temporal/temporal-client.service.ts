import { Injectable, OnModuleInit } from '@nestjs/common';
import { Connection, WorkflowClient } from '@temporalio/client';

// Thin wrapper exposing a connected WorkflowClient as a Nest provider.
@Injectable()
export class TemporalClientService implements OnModuleInit {
  private _client: WorkflowClient;

  async onModuleInit(): Promise<void> {
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
    });
    this._client = new WorkflowClient({
      connection,
      namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
    });
  }

  get client(): WorkflowClient {
    return this._client;
  }
}
