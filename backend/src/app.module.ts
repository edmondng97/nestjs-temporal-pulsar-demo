import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ScheduleModule } from '@nestjs/schedule';
import { CampaignModule } from './modules/campaign/campaign.module';
import { CampaignDeliveryModule } from './modules/campaign-delivery/campaign-delivery.module';
import { PulsarModule } from './libs/pulsar/pulsar.module';
import { ReconciliationModule } from './orchestration/reconciliation/reconciliation.module';
import { TargetingActivity } from './modules/campaign-temporal-scheduler/activities/targeting.activity';
import { DispatchPlayersActivity } from './modules/campaign-delivery-temporal/activities/dispatch-players.activity';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? 'mongodb://localhost:27017/campaign_demo'),
    ScheduleModule.forRoot(),
    CampaignModule,
    CampaignDeliveryModule,
    PulsarModule,
    ReconciliationModule,
  ],
  // Activities are Nest providers so they get full DI; the worker binds their methods.
  providers: [TargetingActivity, DispatchPlayersActivity],
  exports: [TargetingActivity, DispatchPlayersActivity],
})
export class AppModule {}
