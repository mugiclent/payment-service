import { prisma } from '../db/prisma.js';
import { publishPaymentConfirmed, publishPaymentFailed } from '../rabbitmq/publisher.js';
import { assertUuid, assertUuidIfPresent } from '../utils/validate.js';
import { v4 as uuidv4 } from 'uuid';

export interface CashPaymentInput {
  paymentRef: string;
  orgId:      string;
  amount:     bigint;
  currency:   string;
  userId?:    string | null;
  ticketId?:  string | null;
  tripId?:    string | null;
}

export async function handleCashPayment(input: CashPaymentInput): Promise<void> {
  const { paymentRef, orgId, amount, currency, userId, ticketId, tripId } = input;

  try {
    assertUuid('paymentRef', paymentRef);
    assertUuid('orgId',      orgId);
    assertUuidIfPresent('userId',   userId);
    assertUuidIfPresent('ticketId', ticketId);
    assertUuidIfPresent('tripId',   tripId);
  } catch (err) {
    publishPaymentFailed({ paymentRef, method: 'cash', amount, userId, ticketId, reason: (err as Error).message, failedAt: new Date().toISOString(), retryable: false });
    return;
  }

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) {
    if (existing.status === 'CONFIRMED') {
      publishPaymentConfirmed({
        paymentRef,
        method:      'cash',
        amount:      existing.amount,
        currency:    existing.currency,
        userId,
        orgId,
        confirmedAt: existing.updatedAt.toISOString(),
        ticketId:    existing.ticketId,
        tripId:      existing.tripId,
      });
    } else if (existing.status === 'FAILED') {
      publishPaymentFailed({
        paymentRef,
        method:    'cash',
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

  const now = new Date().toISOString();

  await prisma.transaction.create({
    data: {
      id:       uuidv4(),
      paymentRef,
      userId,
      type:     'TICKET_PAYMENT',
      method:   'cash',
      amount,
      currency,
      status:   'CONFIRMED',
      orgId,
      ticketId,
      tripId,
    },
  });

  publishPaymentConfirmed({
    paymentRef,
    method:      'cash',
    amount,
    currency,
    userId,
    orgId,
    confirmedAt: now,
    ticketId,
    tripId,
  });
}
