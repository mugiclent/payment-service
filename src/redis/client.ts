import Redis from 'ioredis';
import { config } from '../config/env.js';

export const redis = new Redis(config.redis.url, {
  lazyConnect: false,
  enableReadyCheck: true,
  maxRetriesPerRequest: null,
});

redis.on('error', (err: Error) => {
  console.error('[redis] Connection error:', err.message);
});

redis.on('connect', () => {
  console.info('[redis] Connected');
});
