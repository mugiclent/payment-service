import { prisma } from '../db/prisma.js';
import { atomicWalletDeduct } from '../redis/walletDeduct.js';
import { seedCache, getBalance } from '../wallet/balance.js';
import {
  publishPaymentConfirmed,
  publishPaymentFailed,
  publishWalletTransactionCompleted,
} from '../rabbitmq/publisher.js';
import { v4 as uuidv4 } from 'uuid';

export interface WalletPaymentInput {
  paymentRef: string;
  userId: string;
  amount: bigint;
  currency: string;
  ticketId?: string | null;
  tripId?: string | null;
  orgId?: string | null;
}

export async function handleWalletPayment(input: WalletPaymentInput): Promise<void> {
  const { paymentRef, userId, amount, currency, ticketId, tripId, orgId } = input;

  // Idempotency check
  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) {
    if (existing.status === 'CONFIRMED') {
      publishPaymentConfirmed({
        paymentRef,
        method:      'wallet',
        amount:      existing.amount,
        currency:    existing.currency,
        userId,
        confirmedAt: existing.updatedAt.toISOString(),
        ticketId:    existing.ticketId,
        tripId:      existing.tripId,
        orgId:       existing.orgId,
      });
    }
    return; // PENDING or FAILED — caller waits or no-ops
  }

  // Attempt atomic deduction from Redis cache
  let result = await atomicWalletDeduct(userId, amount);

  if (result === -2) {
    // Cache miss — load from PostgreSQL and retry once
    const balance = await getBalance(userId);
    await seedCache(userId, balance);
    result = await atomicWalletDeduct(userId, amount);
  }

  if (result === -1) {
    // Redis says insufficient — but a topup may have confirmed between pre-flight
    // and now without updating Redis. Check PostgreSQL before giving up.
    const pgWallet = await prisma.walletBalance.findFirst({ where: { ownerId: userId } });
    if (pgWallet && pgWallet.balance >= amount) {
      // PostgreSQL is ahead of Redis — reseed and retry once
      await seedCache(userId, pgWallet.balance);
      result = await atomicWalletDeduct(userId, amount);
    }

    if (result === -1) {
      publishPaymentFailed({
        paymentRef,
        method:    'wallet',
        amount,
        userId,
        ticketId,
        reason:    'INSUFFICIENT_BALANCE',
        failedAt:  new Date().toISOString(),
        retryable: false,
      });
      return;
    }
  }

  const balanceBefore = BigInt(result) + amount;
  const balanceAfter  = BigInt(result);
  const now = new Date().toISOString();

  await prisma.$transaction([
    prisma.transaction.create({
      data: {
        id:         uuidv4(),
        paymentRef,
        userId,
        type:       'TICKET_PAYMENT',
        method:     'wallet',
        amount,
        currency,
        status:     'CONFIRMED',
        ticketId,
        tripId,
        orgId,
      },
    }),
    prisma.walletBalance.update({
      where: { ownerId_ownerType: { ownerId: userId, ownerType: 'PASSENGER' } },
      data:  { balance: { decrement: amount } },
    }),
    prisma.walletLedger.create({
      data: {
        id:            uuidv4(),
        ownerId:       userId,
        transactionId: paymentRef,
        type:          'DEBIT',
        amount,
        balanceBefore,
        balanceAfter,
        description:   `Ticket payment ${paymentRef}`,
      },
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'WALLET_DEBIT',
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'WALLET_DEBIT',
          payment_ref: paymentRef,
          owner_id:    userId,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency,
          method:      'wallet',
          status:      'CONFIRMED',
          ticket_id:   ticketId ?? null,
          trip_id:     tripId ?? null,
          org_id:      orgId ?? null,
          gateway_ref: null,
          occurred_at: now,
          metadata:    JSON.stringify({ type: 'ticket_payment', ref: paymentRef, ticket_id: ticketId ?? null, trip_id: tripId ?? null, org_id: orgId ?? null }),
        },
      },
    }),
  ]);

  publishPaymentConfirmed({
    paymentRef,
    method:      'wallet',
    amount,
    currency,
    userId,
    confirmedAt: now,
    ticketId,
    tripId,
    orgId,
  });

  publishWalletTransactionCompleted({
    userId,
    newBalance:  balanceAfter,
    type:        'DEBIT',
    amount,
    occurredAt:  now,
  });
}
