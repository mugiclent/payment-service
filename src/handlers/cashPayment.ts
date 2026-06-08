import { prisma } from '../db/prisma.js';
import { publishPaymentConfirmed } from '../rabbitmq/publisher.js';
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
    }
    return;
  }

  const now = new Date().toISOString();

  await prisma.$transaction([
    prisma.transaction.create({
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
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'CASH_PAYMENT_RECORDED',
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'CASH_PAYMENT_RECORDED',
          payment_ref: paymentRef,
          owner_id:    orgId,
          owner_type:  'ORGANISATION',
          amount:      Number(amount),
          currency,
          method:      'cash',
          status:      'CONFIRMED',
          ticket_id:   ticketId ?? null,
          trip_id:     tripId ?? null,
          org_id:      orgId,
          gateway_ref: null,
          occurred_at: now,
          metadata:    null,
        },
      },
    }),
  ]);

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
