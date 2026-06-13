import { prisma } from '../db/prisma.js';
import { seedCache, getBalance } from '../wallet/balance.js';
import { publishPassengerTransaction, publishOrganisationTransaction } from '../rabbitmq/publisher.js';
import { computeFee } from '../payments/fee.js';
import { assertUuid } from '../utils/validate.js';
import { v4 as uuidv4 } from 'uuid';

export interface WalletRefundInput {
  paymentRef:         string;
  originalPaymentRef: string;
  userId:             string;
  amount:             bigint;
  currency:           string;
  ticketId?:          string | null;
  reason?:            string;
}

export async function handleWalletRefund(input: WalletRefundInput): Promise<void> {
  const { paymentRef, originalPaymentRef, userId, amount, currency, ticketId, reason } = input;

  try {
    assertUuid('paymentRef',         paymentRef);
    assertUuid('originalPaymentRef', originalPaymentRef);
    assertUuid('userId',             userId);
  } catch (err) {
    console.warn('[wallet-refund] Invalid UUID, dropping message:', (err as Error).message);
    return;
  }

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) return;

  const original = await prisma.transaction.findUnique({ where: { paymentRef: originalPaymentRef } });
  const orgId = original?.orgId ?? null;

  const feeAmount = computeFee(amount);
  const netAmount = amount - feeAmount;

  const passengerBalanceBefore = await getBalance(userId);
  const passengerBalanceAfter  = passengerBalanceBefore + amount;
  const now = new Date().toISOString();

  // Pre-fetch org wallet balance for ledger entry
  const orgWallet = orgId
    ? await prisma.walletBalance.findFirst({ where: { ownerId: orgId, ownerType: 'ORGANISATION' } })
    : null;
  const orgBalanceBefore = orgWallet?.balance ?? BigInt(0);
  const orgBalanceAfter  = orgBalanceBefore - netAmount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ops: any[] = [
    prisma.transaction.create({
      data: {
        id:         uuidv4(),
        paymentRef,
        userId,
        type:       'REFUND',
        method:     'wallet',
        amount,
        currency,
        status:     'CONFIRMED',
        ticketId,
        feeAmount,
        metadata:   { originalPaymentRef, reason: reason ?? null },
      },
    }),
    // Passenger wallet credit (full amount back)
    prisma.walletBalance.update({
      where: { ownerId_ownerType: { ownerId: userId, ownerType: 'PASSENGER' } },
      data:  { balance: { increment: amount } },
    }),
    prisma.walletLedger.create({
      data: {
        id:            uuidv4(),
        ownerId:       userId,
        transactionId: paymentRef,
        type:          'CREDIT',
        amount,
        balanceBefore: passengerBalanceBefore,
        balanceAfter:  passengerBalanceAfter,
        description:   `Refund for ${originalPaymentRef}`,
      },
    }),
    // Passenger ImmuDB entry
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'WALLET_CREDIT',
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'WALLET_CREDIT',
          payment_ref: paymentRef,
          owner_id:    userId,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency,
          method:      'wallet',
          status:      'CONFIRMED',
          ticket_id:   ticketId ?? null,
          trip_id:     null,
          org_id:      orgId ?? null,
          gateway_ref: null,
          occurred_at: now,
          metadata:    JSON.stringify({ type: 'refund', original_ref: originalPaymentRef, reason: reason ?? null }),
        },
      },
    }),
  ];

  if (orgId && orgWallet) {
    ops.push(
      // Org wallet debit (returns the net amount it received)
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
          description:   `Refund reversal for ${originalPaymentRef}`,
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
            method:      'wallet',
            status:      'CONFIRMED',
            ticket_id:   ticketId ?? null,
            trip_id:     null,
            org_id:      orgId,
            gateway_ref: null,
            occurred_at: now,
            metadata:    JSON.stringify({ type: 'refund_reversal', original_ref: originalPaymentRef }),
          },
        },
      }),
    );
  }

  await prisma.$transaction(ops);

  await seedCache(userId, passengerBalanceAfter);

  publishPassengerTransaction({
    userId,
    newBalance: passengerBalanceAfter,
    movement:   'CREDIT',
    method:     'wallet',
    amount,
    occurredAt: now,
    source:     'refund',
    reference:  paymentRef,
    ticketId:   ticketId ?? null,
  });

  if (orgId && orgWallet) {
    publishOrganisationTransaction({
      orgId,
      newBalance: orgBalanceAfter,
      movement:   'DEBIT',
      amount:     netAmount,
      occurredAt: now,
      source:     'refund',
      reference:  paymentRef,
      ticketId:   ticketId ?? null,
    });
  }
}
