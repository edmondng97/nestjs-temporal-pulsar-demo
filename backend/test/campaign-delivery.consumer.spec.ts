import { Types } from 'mongoose';
import { CampaignDeliveryConsumer } from '../src/libs/pulsar/campaign-delivery.consumer';
import { DELIVERY_STATUS } from '../src/constants';

// Minimal stubs for the consumer's five constructor dependencies
function makeConsumer(overrides: {
  delivery?: object | null;
  epoch?: number;
  casResult?: number;
  deliverOk?: boolean;
} = {}) {
  const { delivery, epoch = 1, casResult = 1, deliverOk = true } = overrides;

  const mockClient = {} as any;

  const mockDeliveryService = {
    findOne: jest.fn().mockResolvedValue(
      delivery !== undefined
        ? delivery
        : { status: DELIVERY_STATUS.IN_PROGRESS, campaignId: new Types.ObjectId() },
    ),
    markSendingIfInProgress: jest.fn().mockResolvedValue({ modifiedCount: casResult }),
    markTerminal: jest.fn().mockResolvedValue(undefined),
  };

  const mockCampaignService = {
    getDispatchEpoch: jest.fn().mockResolvedValue(epoch),
  };

  const mockEvents = { emit: jest.fn() };

  const consumer = new CampaignDeliveryConsumer(
    mockClient,
    mockDeliveryService as any,
    mockCampaignService as any,
    mockEvents as any,
  );

  return { consumer, mockDeliveryService, mockCampaignService, mockEvents };
}

describe('CampaignDeliveryConsumer.handle()', () => {
  const baseMsg = { deliveryId: new Types.ObjectId().toString(), epoch: 1 };

  it('emits REJECTED_STALE when message epoch is stale', async () => {
    // currentEpoch = 2, message epoch = 1 → stale
    const { consumer, mockEvents, mockDeliveryService } = makeConsumer({ epoch: 2 });

    // Access private handle via cast
    await (consumer as any).handle(baseMsg);

    expect(mockEvents.emit).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'REJECTED_STALE', epoch: 1 }),
    );
    expect(mockDeliveryService.markTerminal).not.toHaveBeenCalled();
  });

  it('emits SUCCESS on terminal success path', async () => {
    const { consumer, mockEvents } = makeConsumer({ deliverOk: true });

    // Patch deliverStub to always succeed
    jest.spyOn(consumer as any, 'deliverStub').mockResolvedValue(true);

    await (consumer as any).handle(baseMsg);

    expect(mockEvents.emit).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'SUCCESS' }),
    );
  });

  it('emits FAILED on terminal failure path', async () => {
    const { consumer, mockEvents } = makeConsumer();

    jest.spyOn(consumer as any, 'deliverStub').mockResolvedValue(false);

    await (consumer as any).handle(baseMsg);

    expect(mockEvents.emit).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'FAILED' }),
    );
  });

  it('does not emit when delivery row is missing', async () => {
    const { consumer, mockEvents } = makeConsumer({ delivery: null });

    await (consumer as any).handle(baseMsg);

    expect(mockEvents.emit).not.toHaveBeenCalled();
  });

  it('does not emit when CAS loses', async () => {
    const { consumer, mockEvents } = makeConsumer({ casResult: 0 });

    await (consumer as any).handle(baseMsg);

    expect(mockEvents.emit).not.toHaveBeenCalled();
  });
});
