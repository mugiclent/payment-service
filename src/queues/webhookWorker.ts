import { Worker } from 'bullmq';
import { prisma } from '../db/prisma.js';
import { config } from '../config/env.js';
import { seedCache } from '../wallet/balance.js';
import {
  publishPaymentConfirmed,
  publishPaymentFailed,
  publishTopupConfirmed,
  publishTopupFailed,
  publishWalletTransactionCompleted,
} from '../rabbitmq/publisher.js';
import { computeFee } from '../payments/fee.js';
import type { PaymentWebhookEvent } from '../gateway/types.js';
import { v4 as uuidv4 } from 'uuid';

async function handleTopupConfirmation(
  trx: { id: string; paymentRef: string; topupId: string | null; userId: string | null; amount: bigint; currency: string },
  gatewayRef?: string,
): Promise<void> {
  const userId = trx.userId!;
  const amount = trx.amount;
  const now    = new Date().toISOString();

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

  await seedCache(userId, balanceAfter);

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

async function handleMomoTicketConfirmation(
  trx: {
    paymentRef: string;
    amount: bigint;
    currency: string;
    method: string;
    userId: string | null;
    phone: string | null;
    ticketId: string | null;
    tripId: string | null;
    orgId: string | null;
  },
  gatewayRef: string | undefined,
  now: string,
): Promise<void> {
  const { paymentRef, amount, currency, method, ticketId, tripId, orgId } = trx;

  const feeAmount = computeFee(amount);
  const netAmount = amount - feeAmount;

  // Pre-fetch org wallet balance for ledger entry
  const orgWallet = orgId
    ? await prisma.walletBalance.findFirst({ where: { ownerId: orgId, ownerType: 'ORGANISATION' } })
    : null;
  const orgBalanceBefore = orgWallet?.balance ?? BigInt(0);
  const orgBalanceAfter  = orgBalanceBefore + netAmount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ops: any[] = [
    prisma.transaction.update({
      where: { paymentRef },
      data:  { status: 'CONFIRMED', gatewayRef: gatewayRef ?? null, feeAmount },
    }),
    // Platform fee ImmuDB entry
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'PLATFORM_FEE',
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'PLATFORM_FEE',
          payment_ref: paymentRef,
          owner_id:    'katisha-platform',
          owner_type:  'PLATFORM',
          amount:      Number(feeAmount),
          currency,
          method,
          status:      'CONFIRMED',
          ticket_id:   ticketId   ?? null,
          trip_id:     tripId     ?? null,
          org_id:      orgId      ?? null,
          gateway_ref: gatewayRef ?? null,
          occurred_at: now,
          metadata:    null,
        },
      },
    }),
  ];

  if (orgId) {
    ops.push(
      prisma.walletBalance.upsert({
        where:  { ownerId_ownerType: { ownerId: orgId, ownerType: 'ORGANISATION' } },
        update: { balance: { increment: netAmount } },
        create: {
          id:        uuidv4(),
          ownerId:   orgId,
          ownerType: 'ORGANISATION',
          balance:   netAmount,
        },
      }),
      prisma.walletLedger.create({
        data: {
          id:            uuidv4(),
          ownerId:       orgId,
          transactionId: paymentRef,
          type:          'CREDIT',
          amount:        netAmount,
          balanceBefore: orgBalanceBefore,
          balanceAfter:  orgBalanceAfter,
          description:   `MoMo ticket payment net ${paymentRef}`,
        },
      }),
      // Org ImmuDB entry
      prisma.outboxEntry.create({
        data: {
          id:         uuidv4(),
          eventType:  'WALLET_CREDIT',
          paymentRef: `${paymentRef}-org`,
          payload: {
            id:          uuidv4(),
            event_type:  'WALLET_CREDIT',
            payment_ref: paymentRef,
            owner_id:    orgId,
            owner_type:  'ORGANISATION',
            amount:      Number(netAmount),
            currency,
            method,
            status:      'CONFIRMED',
            ticket_id:   ticketId   ?? null,
            trip_id:     tripId     ?? null,
            org_id:      orgId,
            gateway_ref: gatewayRef ?? null,
            occurred_at: now,
            metadata:    null,
          },
        },
      }),
    );
  }

  await prisma.$transaction(ops);

  publishPaymentConfirmed({
    paymentRef,
    method,
    amount,
    currency,
    userId:    trx.userId,
    phone:     trx.phone,
    ticketId,
    tripId,
    orgId,
    confirmedAt: now,
    gatewayRef,
    feeAmount,
    netAmount,
  });
}

async function handleMomoRefundConfirmation(
  trx: {
    paymentRef: string;
    amount: bigint;
    currency: string;
    method: string;
    orgId: string | null;
    metadata: unknown;
  },
  gatewayRef: string | undefined,
  now: string,
): Promise<void> {
  const { paymentRef, amount, currency, method } = trx;
  const meta = trx.metadata as Record<string, string> | null;
  const originalPaymentRef = meta?.['originalPaymentRef'] ?? null;

  // Resolve orgId from original transaction if not on the refund record
  let orgId = trx.orgId;
  if (!orgId && originalPaymentRef) {
    const original = await prisma.transaction.findUnique({ where: { paymentRef: originalPaymentRef } });
    orgId = original?.orgId ?? null;
  }

  const feeAmount = computeFee(amount);
  const netAmount = amount - feeAmount;

  const orgWallet = orgId
    ? await prisma.walletBalance.findFirst({ where: { ownerId: orgId, ownerType: 'ORGANISATION' } })
    : null;
  const orgBalanceBefore = orgWallet?.balance ?? BigInt(0);
  const orgBalanceAfter  = orgBalanceBefore - netAmount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ops: any[] = [
    prisma.transaction.update({
      where: { paymentRef },
      data:  { status: 'CONFIRMED', gatewayRef: gatewayRef ?? null, feeAmount },
    }),
  ];

  if (orgId && orgWallet) {
    ops.push(
      prisma.walletBalance.update({
        where: { ownerId_ownerType: { ownerId: orgId, ownerType: 'ORGANISATION' } },
        data:  { balance: { decrement: netAmount } },
      }),
      prisma.walletLedger.create({
        data: {
          id:            uuidv4(),
          ownerId:       orgId,
          transactionId: paymentRef,
          type:          'DEBIT',
          amount:        netAmount,
          balanceBefore: orgBalanceBefore,
          balanceAfter:  orgBalanceAfter,
          description:   `MoMo refund reversal for ${originalPaymentRef ?? paymentRef}`,
        },
      }),
      // Org ImmuDB entry
      prisma.outboxEntry.create({
        data: {
          id:         uuidv4(),
          eventType:  'WALLET_DEBIT',
          paymentRef: `${paymentRef}-org`,
          payload: {
            id:          uuidv4(),
            event_type:  'WALLET_DEBIT',
            payment_ref: paymentRef,
            owner_id:    orgId,
            owner_type:  'ORGANISATION',
            amount:      Number(netAmount),
            currency,
            method,
            status:      'CONFIRMED',
            ticket_id:   null,
            trip_id:     null,
            org_id:      orgId,
            gateway_ref: gatewayRef ?? null,
            occurred_at: now,
            metadata:    JSON.stringify({ type: 'refund_reversal', original_ref: originalPaymentRef }),
          },
        },
      }),
    );
  }

  await prisma.$transaction(ops);
}

export function startWebhookWorker(): Worker {
  const worker = new Worker(
    'payment-webhooks',
    async (job) => {
      // Guard against stale ttl-fallback jobs left in this queue from before the queue split
      if (job.name === 'ttl-fallback') return;

      const event = job.data as PaymentWebhookEvent;
      const { paymentRef, gatewayRef, status, failureReason } = event;

      const trx = await prisma.transaction.findUnique({ where: { paymentRef: paymentRef } });
      if (!trx) {
        console.warn('[webhook] Unknown paymentRef:', paymentRef);
        return;
      }

      if (trx.status === 'CONFIRMED' || trx.status === 'REFUNDED') return;

      const now = new Date().toISOString();

      if (status === 'SUCCESSFUL') {
        if (trx.type === 'WALLET_TOPUP') {
          await handleTopupConfirmation(
            { id: trx.id, paymentRef: trx.paymentRef, topupId: trx.topupId, userId: trx.userId, amount: trx.amount, currency: trx.currency },
            gatewayRef,
          );
          return;
        }

        if (trx.type === 'REFUND') {
          await handleMomoRefundConfirmation(
            { paymentRef: trx.paymentRef, amount: trx.amount, currency: trx.currency, method: trx.method, orgId: trx.orgId, metadata: trx.metadata },
            gatewayRef,
            now,
          );
          return;
        }

        // TICKET_PAYMENT via MoMo
        await handleMomoTicketConfirmation(
          { paymentRef: trx.paymentRef, amount: trx.amount, currency: trx.currency, method: trx.method, userId: trx.userId, phone: trx.phone, ticketId: trx.ticketId, tripId: trx.tripId, orgId: trx.orgId },
          gatewayRef,
          now,
        );
      } else {
        const reason = failureReason ?? 'MOMO_FAILED';

        await prisma.transaction.update({
          where: { paymentRef: paymentRef },
          data:  { status: 'FAILED', gatewayRef: gatewayRef ?? null, failureReason: reason },
        });

        if (trx.type === 'WALLET_TOPUP') {
          publishTopupFailed({
            topupId:   trx.topupId ?? paymentRef,
            topupRef:  paymentRef,
            userId:    trx.userId!,
            amount:    trx.amount,
            reason,
            failedAt:  now,
          });
        } else {
          publishPaymentFailed({
            paymentRef: paymentRef,
            method:     trx.method,
            amount:     trx.amount,
            userId:     trx.userId,
            phone:      trx.phone,
            ticketId:   trx.ticketId,
            reason,
            failedAt:   now,
            retryable:  false,
          });
        }
      }
    },
    { connection: { url: config.redis.url } },
  );

  worker.on('failed', (job, err) => {
    console.error('[webhook-worker] Job failed:', job?.id, err.message);
  });

  return worker;
}
