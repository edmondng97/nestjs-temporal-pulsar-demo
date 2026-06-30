import { Injectable } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService {
  private readonly client: Redis;

  constructor(url = process.env.REDIS_URL ?? 'redis://localhost:6379') {
    this.client = new Redis(url);
  }

  private pauseKey(campaignId: string): string {
    return `campaign:pause:${campaignId}`;
  }

  async isPaused(campaignId: string): Promise<boolean> {
    return (await this.client.get(this.pauseKey(campaignId))) === '1';
  }

  async setPaused(campaignId: string, paused: boolean): Promise<void> {
    // Store '1' for paused; delete the key when unpaused to keep Redis clean.
    if (paused) await this.client.set(this.pauseKey(campaignId), '1');
    else await this.client.del(this.pauseKey(campaignId));
  }
}
