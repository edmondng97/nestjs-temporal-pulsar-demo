import { Injectable } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService {
  private readonly client: Redis;

  // Parameterless so Nest can construct it via DI. A constructor parameter (even
  // with a default) makes Nest try to resolve it as a provider and fail at boot;
  // the connection URL is environment config, not an injectable dependency.
  constructor() {
    this.client = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
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
