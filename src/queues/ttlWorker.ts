import { Worker } from 'bullmq';
import { prisma } from '../db/prisma.js';
import { config } from '../config/env.js';
import { fdiGetTransactionInfo } from '../gateway/fdi/client.js';
import { normaliseFdi } from '../gateway/normalisers/fdi.js';
import { webhookQueue, ttlQueue } from './webhookQueue.js';
import { publishTopupFailed, publishPaymentFailed } from '../rabbitmq/publisher.js';

const MAX_TTL_ATTEMPTS = 3;

export function startTtlWorker(): Worker {
  const worker = new Worker(
    'payment-ttl',
    async (job) => {
      const { paymentRef, attempt = 0 } = job.data as { paymentRef: string; provider: string; attempt?: number };

      const trx = await prisma.transaction.findUnique({ where: { paymentRef } });
      if (!trx || trx.status !== 'PENDING') return;

      try {
        const infoRes = await fdiGetTransactionInfo(paymentRef);

        // Still pending at FDI
        if (infoRes.data?.trxStatus === 'pending') {
          const nextAttempt = attempt + 1;

          if (nextAttempt > MAX_TTL_ATTEMPTS) {
            const reason = 'PAYMENT_UNRESOLVED';
            const now    = new Date().toISOString();
            await prisma.transaction.update({
              where: { paymentRef },
              data:  { status: 'FAILED', failureReason: reason },
            });
            if (trx.type === 'WALLET_TOPUP') {
              publishTopupFailed({
                topupId:  trx.topupId ?? paymentRef,
                topupRef: paymentRef,
                userId:   trx.userId!,
                amount:   trx.amount,
                reason,
                failedAt: now,
              });
            } else {
              publishPaymentFailed({
                paymentRef,
                method:    trx.method,
                amount:    trx.amount,
                userId:    trx.userId,
                phone:     trx.phone,
                ticketId:  trx.ticketId,
                reason,
                failedAt:  now,
                retryable: false,
              });
            }
            return;
          }

          await prisma.transaction.update({
            where: { paymentRef },
            data:  { metadata: { ttlChecked: new Date().toISOString(), status: 'pending', attempt: nextAttempt } },
          });
          await ttlQueue.add(
            'ttl-fallback',
            { paymentRef, provider: 'fdi', attempt: nextAttempt },
            { delay: 120_000, jobId: `ttl-${paymentRef}-${nextAttempt}` },
          );
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
