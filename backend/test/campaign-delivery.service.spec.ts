import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose, { Connection, Types } from 'mongoose';
import { CampaignDeliverySchema } from '../src/schemas/campaign-delivery.schema';
import { CampaignDeliveryService } from '../src/modules/campaign-delivery/campaign-delivery.service';
import { DELIVERY_STATUS } from '../src/constants';

describe('CampaignDeliveryService CAS', () => {
  let mongod: MongoMemoryServer;
  let conn: Connection;
  let service: CampaignDeliveryService;

  beforeAll(async () => {
    mongod = await MongoMemoryServer.create();
    conn = (await mongoose.createConnection(mongod.getUri()).asPromise());
    const model = conn.model('CampaignDelivery', CampaignDeliverySchema);
    service = new CampaignDeliveryService(model as any);
  });
  afterAll(async () => { await conn.close(); await mongod.stop(); });

  it('markInProgressIfPending wins once, loses on re-run', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    const first = await service.markInProgressIfPending(d._id);
    const second = await service.markInProgressIfPending(d._id);
    expect(first.modifiedCount).toBe(1);
    expect(second.modifiedCount).toBe(0);
  });

  it('markSendingIfInProgress only transitions from IN_PROGRESS', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    expect((await service.markSendingIfInProgress(d._id)).modifiedCount).toBe(0); // still PENDING
    await service.markInProgressIfPending(d._id);
    expect((await service.markSendingIfInProgress(d._id)).modifiedCount).toBe(1);
  });

  it('markTerminal writes status + completedAt', async () => {
    const cid = new Types.ObjectId();
    await service.createMany(cid, 1);
    const [d] = await service.findPendingPage(cid, 10);
    await service.markTerminal({ deliveryId: d._id, status: DELIVERY_STATUS.SUCCESS, completedAt: new Date() });
    const after = await service.findOne({ _id: d._id });
    expect(after!.status).toBe(DELIVERY_STATUS.SUCCESS);
    expect(after!.completedAt).toBeDefined();
  });
});
