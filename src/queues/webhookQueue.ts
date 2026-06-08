import { Queue } from 'bullmq';
import { redis } from '../redis/client.js';

export const webhookQueue = new Queue('payment-webhooks', {
  connection: redis,
  defaultJobOptions: {
    attempts:         3,
    backoff:          { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 1000 },
    removeOnFail:     { count: 5000 },
  },
});
