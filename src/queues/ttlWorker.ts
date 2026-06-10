import { Worker } from 'bullmq';
import { prisma } from '../db/prisma.js';
import { config } from '../config/env.js';
import { fdiGetTransactionInfo } from '../gateway/fdi/client.js';
import { normaliseFdi } from '../gateway/normalisers/fdi.js';
import { webhookQueue, ttlQueue } from './webhookQueue.js';

export function startTtlWorker(): Worker {
  const worker = new Worker(
    'payment-ttl',
    async (job) => {
      const { paymentRef } = job.data as { paymentRef: string; provider: string };

      const trx = await prisma.transaction.findUnique({ where: { paymentRef } });
      if (!trx || trx.status !== 'PENDING') return;

      try {
        const infoRes = await fdiGetTransactionInfo(paymentRef);

        // Still pending at FDI — update metadata, wait for eventual webhook
        if (infoRes.data?.trxStatus === 'pending') {
          await prisma.transaction.update({
            where: { paymentRef },
            data:  { metadata: { ttlChecked: new Date().toISOString(), status: 'pending' } },
          });
          return;
        }

        // Build a synthetic event using the same normaliser as live webhooks.
        // FDI error responses (e.g. "Invalid trxRef") don't include trxRef —
        // fall back to the paymentRef we already know so the webhook worker
        // can always find and update the transaction.
        const event = normaliseFdi(infoRes);
        if (!event.paymentRef) event.paymentRef = paymentRef;
        await webhookQueue.add('payment-event', event);
      } catch (err) {
        console.warn('[ttl-worker] FDI status poll failed for', paymentRef, (err as Error).message);
      }
    },
    { connection: { url: config.redis.url } },
  );

  worker.on('failed', (job, err) => {
    console.error('[ttl-worker] Job failed:', job?.id, err.message);
  });

  return worker;
}
