import { ReconciliationService } from '../src/orchestration/reconciliation/reconciliation.service';
import { DELIVERY_STATUS } from '../src/constants';
import { Types } from 'mongoose';

describe('ReconciliationService', () => {
  it('marks campaign completed when no non-terminal deliveries remain', async () => {
    const id = new Types.ObjectId();
    const campaignService = {
      listInProgress: jest.fn().mockResolvedValue([{ _id: id }]),
      markCompleted: jest.fn().mockResolvedValue(undefined),
    };
    const deliveryService = {
      countByStatus: jest.fn().mockImplementation((_cid, status) =>
        status === DELIVERY_STATUS.PENDING || status === DELIVERY_STATUS.IN_PROGRESS || status === DELIVERY_STATUS.SENDING
          ? Promise.resolve(0) : Promise.resolve(5)),
    };
    const svc = new ReconciliationService(campaignService as any, deliveryService as any);
    await svc.reconcileOnce();
    expect(campaignService.markCompleted).toHaveBeenCalledWith(id);
  });

  it('does NOT complete while non-terminal deliveries remain', async () => {
    const id = new Types.ObjectId();
    const campaignService = {
      listInProgress: jest.fn().mockResolvedValue([{ _id: id }]),
      markCompleted: jest.fn(),
    };
    const deliveryService = {
      countByStatus: jest.fn().mockImplementation((_cid, status) =>
        status === DELIVERY_STATUS.IN_PROGRESS ? Promise.resolve(3) : Promise.resolve(0)),
    };
    const svc = new ReconciliationService(campaignService as any, deliveryService as any);
    await svc.reconcileOnce();
    expect(campaignService.markCompleted).not.toHaveBeenCalled();
  });
});
