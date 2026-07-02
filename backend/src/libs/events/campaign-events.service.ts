import { Injectable } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';

export interface CampaignDeliveryEvent {
  campaignId: string;
  deliveryId: string;
  outcome: 'SUCCESS' | 'FAILED' | 'REJECTED_STALE';
  epoch: number;
  ts: string;
}

// Plain Subject (not Replay): SSE clients only care about live traffic.
@Injectable()
export class CampaignEventsService {
  private readonly subject = new Subject<CampaignDeliveryEvent>();

  emit(event: CampaignDeliveryEvent): void {
    this.subject.next(event);
  }

  stream(): Observable<CampaignDeliveryEvent> {
    return this.subject.asObservable();
  }
}
