import { prisma } from '../db/prisma.js';
import { seedCache, getBalance } from '../wallet/balance.js';
import { publishWalletTransactionCompleted } from '../rabbitmq/publisher.js';
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

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) return;

  const balanceBefore = await getBalance(userId);
  const balanceAfter  = balanceBefore + amount;
  const now = new Date().toISOString();

  await prisma.$transaction([
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
        metadata:   { originalPaymentRef, reason: reason ?? null },
      },
    }),
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
        balanceBefore,
        balanceAfter,
        description:   `REFUND for ${originalPaymentRef}`,
      },
    }),
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
          org_id:      null,
          gateway_ref: null,
          occurred_at: now,
          metadata:    JSON.stringify({ originalPaymentRef, reason }),
        },
      },
    }),
  ]);

  await seedCache(userId, balanceAfter);

  publishWalletTransactionCompleted({
    userId,
    newBalance:  balanceAfter,
    type:        'CREDIT',
    amount,
    occurredAt:  now,
  });
}
