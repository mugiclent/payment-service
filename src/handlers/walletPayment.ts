import { prisma } from '../db/prisma.js';
import { atomicWalletDeduct } from '../redis/walletDeduct.js';
import { seedCache, getBalance } from '../wallet/balance.js';
import {
  publishPaymentConfirmed,
  publishPaymentFailed,
  publishWalletTransactionCompleted,
} from '../rabbitmq/publisher.js';
import { computeFee } from '../payments/fee.js';
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
        feeAmount:   existing.feeAmount,
        netAmount:   existing.feeAmount != null ? existing.amount - existing.feeAmount : null,
      });
    } else if (existing.status === 'FAILED') {
      publishPaymentFailed({
        paymentRef,
        method:    'wallet',
        amount:    existing.amount,
        userId,
        ticketId:  existing.ticketId,
        reason:    existing.failureReason ?? 'PAYMENT_FAILED',
        failedAt:  existing.updatedAt.toISOString(),
        retryable: false,
      });
    }
    // PENDING — still in flight
    return;
  }

  // Attempt atomic deduction from Redis cache
  let result = await atomicWalletDeduct(userId, amount);

  if (result === -2) {
    const balance = await getBalance(userId);
    await seedCache(userId, balance);
    result = await atomicWalletDeduct(userId, amount);
  }

  if (result === -1) {
    const pgWallet = await prisma.walletBalance.findFirst({ where: { ownerId: userId } });
    if (pgWallet && pgWallet.balance >= amount) {
      await seedCache(userId, pgWallet.balance);
      result = await atomicWalletDeduct(userId, amount);
    }

    if (result === -1) {
      await prisma.transaction.create({
        data: {
          id:            uuidv4(),
          paymentRef,
          userId,
          type:          'TICKET_PAYMENT',
          method:        'wallet',
          amount,
          currency,
          status:        'FAILED',
          ticketId,
          tripId,
          orgId,
          failureReason: 'INSUFFICIENT_BALANCE',
        },
      });
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

  const feeAmount     = computeFee(amount);
  const netAmount     = amount - feeAmount;
  const balanceBefore = BigInt(result) + amount;
  const balanceAfter  = BigInt(result);
  const now           = new Date().toISOString();

  // Pre-fetch org wallet balance for ledger entry (best-effort; increment is atomic)
  const orgWallet = orgId
    ? await prisma.walletBalance.findFirst({ where: { ownerId: orgId, ownerType: 'ORGANISATION' } })
    : null;
  const orgBalanceBefore = orgWallet?.balance ?? BigInt(0);
  const orgBalanceAfter  = orgBalanceBefore + netAmount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ops: any[] = [
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
        feeAmount,
      },
    }),
    // Passenger wallet debit
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
    // Passenger ImmuDB entry
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
          trip_id:     tripId  ?? null,
          org_id:      orgId   ?? null,
          gateway_ref: null,
          occurred_at: now,
          metadata:    null,
        },
      },
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
          method:      'wallet',
          status:      'CONFIRMED',
          ticket_id:   ticketId ?? null,
          trip_id:     tripId  ?? null,
          org_id:      orgId   ?? null,
          gateway_ref: null,
          occurred_at: now,
          metadata:    null,
        },
      },
    }),
  ];

  if (orgId) {
    ops.push(
      // Org wallet credit (net after fee)
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
          description:   `Ticket payment net ${paymentRef}`,
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
            method:      'wallet',
            status:      'CONFIRMED',
            ticket_id:   ticketId ?? null,
            trip_id:     tripId  ?? null,
            org_id:      orgId,
            gateway_ref: null,
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
    method:      'wallet',
    amount,
    currency,
    userId,
    confirmedAt: now,
    ticketId,
    tripId,
    orgId,
    feeAmount,
    netAmount,
  });

  publishWalletTransactionCompleted({
    userId,
    newBalance:  balanceAfter,
    type:        'DEBIT',
    amount,
    occurredAt:  now,
  });
}
