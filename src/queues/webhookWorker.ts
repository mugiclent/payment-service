import { Worker } from 'bullmq';
import { prisma } from '../db/prisma.js';
import { redis } from '../redis/client.js';
import { seedCache } from '../wallet/balance.js';
import {
  publishPaymentConfirmed,
  publishPaymentFailed,
  publishTopupConfirmed,
  publishTopupFailed,
  publishWalletTransactionCompleted,
} from '../rabbitmq/publisher.js';
import type { PaymentWebhookEvent } from '../gateway/types.js';
import { v4 as uuidv4 } from 'uuid';

async function handleTopupConfirmation(
  trx: { id: string; paymentRef: string; userId: string | null; amount: bigint; currency: string },
  gatewayRef?: string,
): Promise<void> {
  const userId = trx.userId!;
  const amount = trx.amount;
  const now    = new Date().toISOString();

  // Get current balance for ledger entry
  const wallet = await prisma.walletBalance.findFirst({ where: { ownerId: userId } });
  if (!wallet) throw new Error(`Wallet not found for user ${userId}`);

  const balanceBefore = wallet.balance;
  const balanceAfter  = balanceBefore + amount;

  await prisma.$transaction([
    prisma.transaction.update({
      where: { paymentRef: trx.paymentRef },
      data:  { status: 'CONFIRMED', gatewayRef: gatewayRef ?? null },
    }),
    prisma.walletBalance.update({
      where: { ownerId_ownerType: { ownerId: userId, ownerType: 'PASSENGER' } },
      data:  { balance: { increment: amount } },
    }),
    prisma.walletLedger.create({
      data: {
        id:            uuidv4(),
        ownerId:       userId,
        transactionId: trx.paymentRef,
        type:          'CREDIT',
        amount,
        balanceBefore,
        balanceAfter,
        description:   `Wallet top-up ${trx.paymentRef}`,
      },
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'WALLET_CREDIT',
        paymentRef: trx.paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'WALLET_CREDIT',
          payment_ref: trx.paymentRef,
          owner_id:    userId,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency:    trx.currency,
          method:      'mtn',
          status:      'CONFIRMED',
          ticket_id:   null,
          trip_id:     null,
          org_id:      null,
          gateway_ref: gatewayRef ?? null,
          occurred_at: now,
          metadata:    JSON.stringify({ type: 'topup', ref: trx.paymentRef, gateway_ref: gatewayRef ?? null }),
        },
      },
    }),
  ]);

  publishTopupConfirmed({
    topupId:     trx.topupId ?? trx.paymentRef,
    topupRef:    trx.paymentRef,
    userId,
    amount,
    newBalance:  balanceAfter,
    confirmedAt: now,
  });

  publishWalletTransactionCompleted({
    userId,
    newBalance:  balanceAfter,
    type:        'CREDIT',
    amount,
    occurredAt:  now,
  });
}

export function startWebhookWorker(): Worker {
  const worker = new Worker(
    'payment-webhooks',
    async (job) => {
      if (job.name === 'ttl-fallback') {
        // Handled by ttlWorker — skip here to avoid double processing
        return;
      }

      const event = job.data as PaymentWebhookEvent;
      const { internalRef, gatewayRef, status } = event;

      const trx = await prisma.transaction.findUnique({
        where: { paymentRef: internalRef },
      });

      if (!trx) {
        console.warn('[webhook] Unknown paymentRef:', internalRef);
        return;
      }

      if (trx.status === 'CONFIRMED' || trx.status === 'REFUNDED') return;

      const now = new Date().toISOString();

      if (status === 'SUCCESSFUL') {
        if (trx.type === 'WALLET_TOPUP') {
          await handleTopupConfirmation(
            { id: trx.id, paymentRef: trx.paymentRef, userId: trx.userId, amount: trx.amount, currency: trx.currency },
            gatewayRef,
          );
          return;
        }

        const eventType =
          trx.type === 'REFUND'
            ? 'REFUND_CONFIRMED'
            : trx.method === 'mtn'
              ? 'MTN_PAYMENT_CONFIRMED'
              : 'AIRTEL_PAYMENT_CONFIRMED';

        await prisma.$transaction([
          prisma.transaction.update({
            where: { paymentRef: internalRef },
            data:  { status: 'CONFIRMED', gatewayRef: gatewayRef ?? null },
          }),
          prisma.outboxEntry.create({
            data: {
              id:         uuidv4(),
              eventType,
              paymentRef: internalRef,
              payload: {
                id:          uuidv4(),
                event_type:  eventType,
                payment_ref: internalRef,
                owner_id:    trx.userId ?? trx.phone ?? '',
                owner_type:  'PASSENGER',
                amount:      Number(trx.amount),
                currency:    trx.currency,
                method:      trx.method,
                status:      'CONFIRMED',
                ticket_id:   trx.ticketId ?? null,
                trip_id:     trx.tripId   ?? null,
                org_id:      trx.orgId    ?? null,
                gateway_ref: gatewayRef   ?? null,
                occurred_at: now,
                metadata:    null,
              },
            },
          }),
        ]);

        if (trx.type !== 'REFUND') {
          publishPaymentConfirmed({
            paymentRef:  internalRef,
            method:      trx.method,
            amount:      trx.amount,
            currency:    trx.currency,
            userId:      trx.userId,
            phone:       trx.phone,
            ticketId:    trx.ticketId,
            tripId:      trx.tripId,
            orgId:       trx.orgId,
            confirmedAt: now,
            gatewayRef,
          });
        }
      } else {
        const failedEventType =
          trx.method === 'mtn'    ? 'MTN_PAYMENT_REQUESTED'    :
          trx.method === 'airtel' ? 'AIRTEL_PAYMENT_REQUESTED' :
                                    'REFUND_INITIATED';

        await prisma.$transaction([
          prisma.transaction.update({
            where: { paymentRef: internalRef },
            data:  { status: 'FAILED', gatewayRef: gatewayRef ?? null },
          }),
          prisma.outboxEntry.create({
            data: {
              id:         uuidv4(),
              eventType:  `${failedEventType}_FAILED`,
              paymentRef: internalRef,
              payload: {
                id:          uuidv4(),
                event_type:  `${failedEventType}_FAILED`,
                payment_ref: internalRef,
                owner_id:    trx.userId ?? trx.phone ?? '',
                owner_type:  'PASSENGER',
                amount:      Number(trx.amount),
                currency:    trx.currency,
                method:      trx.method,
                status:      'FAILED',
                ticket_id:   trx.ticketId ?? null,
                trip_id:     trx.tripId   ?? null,
                org_id:      trx.orgId    ?? null,
                gateway_ref: gatewayRef   ?? null,
                occurred_at: now,
                metadata:    null,
              },
            },
          }),
        ]);

        if (trx.type === 'WALLET_TOPUP') {
          publishTopupFailed({
            topupId:   trx.topupId ?? internalRef,
            topupRef:  internalRef,
            userId:    trx.userId!,
            amount:    trx.amount,
            reason:    'MOMO_FAILED',
            failedAt:  now,
          });
        } else {
          publishPaymentFailed({
            paymentRef: internalRef,
            method:     trx.method,
            amount:     trx.amount,
            userId:     trx.userId,
            phone:      trx.phone,
            ticketId:   trx.ticketId,
            reason:     'MOMO_FAILED',
            failedAt:   now,
            retryable:  false,
          });
        }
      }
    },
    { connection: redis },
  );

  worker.on('failed', (job, err) => {
    console.error('[webhook-worker] Job failed:', job?.id, err.message);
  });

  return worker;
}
