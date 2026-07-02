import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { startTemporalWorkers } from './libs/temporal/temporal-worker.bootstrap';
import { CampaignDeliveryConsumer } from './libs/pulsar/campaign-delivery.consumer';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  // Console dev server (Vite default port). Demo page is static and calls no API.
  app.enableCors({ origin: ['http://localhost:5173'] });
  await app.listen(3000);

  // Start the Pulsar consume loop and the Temporal workers alongside the HTTP API.
  await app.get(CampaignDeliveryConsumer).start();
  await startTemporalWorkers(app);

  // eslint-disable-next-line no-console
  console.log('API on :3000, Temporal workers + Pulsar consumer running');
}
void bootstrap();
