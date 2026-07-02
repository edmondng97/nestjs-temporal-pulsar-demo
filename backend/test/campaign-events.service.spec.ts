import { CampaignEventsService, CampaignDeliveryEvent } from '../src/libs/events/campaign-events.service';

describe('CampaignEventsService', () => {
  it('delivers emitted events to subscribers', () => {
    const svc = new CampaignEventsService();
    const seen: CampaignDeliveryEvent[] = [];
    const sub = svc.stream().subscribe((e) => seen.push(e));
    const event: CampaignDeliveryEvent = {
      campaignId: 'c1', deliveryId: 'd1', outcome: 'SUCCESS', epoch: 0, ts: '2026-07-02T00:00:00.000Z',
    };
    svc.emit(event);
    sub.unsubscribe();
    expect(seen).toEqual([event]);
  });

  it('does not replay past events to late subscribers', () => {
    const svc = new CampaignEventsService();
    svc.emit({ campaignId: 'c1', deliveryId: 'd1', outcome: 'FAILED', epoch: 1, ts: 't' });
    const seen: CampaignDeliveryEvent[] = [];
    const sub = svc.stream().subscribe((e) => seen.push(e));
    sub.unsubscribe();
    expect(seen).toEqual([]);
  });
});
