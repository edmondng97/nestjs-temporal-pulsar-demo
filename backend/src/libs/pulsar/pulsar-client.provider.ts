import { Provider } from '@nestjs/common';
import { Client } from 'pulsar-client';
import { TOKENS } from '../../constants';

// A single Pulsar Client shared by producer + consumer. Closed on shutdown by Nest.
export const PulsarClientProvider: Provider = {
  provide: TOKENS.PULSAR_CLIENT,
  useFactory: () =>
    new Client({ serviceUrl: process.env.PULSAR_URL ?? 'pulsar://localhost:6650' }),
};
