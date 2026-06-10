import { Queue } from 'bullmq';
import { config } from '../config/env.js';

const connection = { url: config.redis.url };

export const webhookQueue = new Queue('payment-webhooks', {
  connection,
  defaultJobOptions: {
    attempts:         3,
    backoff:          { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 1000 },
    removeOnFail:     { count: 5000 },
  },
});

export const ttlQueue = new Queue('payment-ttl', {
  connection,
  defaultJobOptions: {
    attempts:         1,
    removeOnComplete: { count: 500 },
    removeOnFail:     { count: 500 },
  },
});
