import { RedisService } from '../src/libs/redis/redis.service';

// Uses ioredis-mock to avoid a live redis in unit tests.
jest.mock('ioredis', () => require('ioredis-mock'));

describe('RedisService pause flag', () => {
  let svc: RedisService;
  beforeAll(() => { svc = new RedisService(); });

  it('defaults to not paused', async () => {
    expect(await svc.isPaused('c1')).toBe(false);
  });
  it('reflects setPaused', async () => {
    await svc.setPaused('c1', true);
    expect(await svc.isPaused('c1')).toBe(true);
    await svc.setPaused('c1', false);
    expect(await svc.isPaused('c1')).toBe(false);
  });
});
