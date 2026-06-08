import { prisma } from '../db/prisma.js';
import { fdiRefund } from '../gateway/fdi/client.js';
import { webhookQueue } from '../queues/webhookQueue.js';
import { v4 as uuidv4 } from 'uuid';

export interface MomoRefundInput {
  paymentRef:         string;
  originalPaymentRef: string;
  phone:              string;
  userId?:            string | null;
  amount:             bigint;
  currency:           string;
  ticketId?:          string | null;
  reason?:            string;
}

export async function handleMomoRefund(input: MomoRefundInput): Promise<void> {
  const { paymentRef, originalPaymentRef, phone, userId, amount, currency, ticketId, reason } =
    input;

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) return;

  const original = await prisma.transaction.findUnique({
    where: { paymentRef: originalPaymentRef },
  });
  if (!original || original.status !== 'CONFIRMED') {
    throw new Error(
      `Original transaction ${originalPaymentRef} not found or not CONFIRMED`,
    );
  }

  await prisma.$transaction([
    prisma.transaction.create({
      data: {
        id:         uuidv4(),
        paymentRef,
        userId,
        phone,
        type:       'REFUND',
        method:     original.method,
        amount,
        currency,
        status:     'PENDING',
        provider:   'fdi',
        ticketId,
        metadata:   { originalPaymentRef, reason: reason ?? null },
      },
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'REFUND_INITIATED',
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  'REFUND_INITIATED',
          payment_ref: paymentRef,
          owner_id:    userId ?? phone,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency,
          method:      original.method,
          status:      'PENDING',
          ticket_id:   ticketId ?? null,
          trip_id:     null,
          org_id:      null,
          gateway_ref: original.gatewayRef ?? null,
          occurred_at: new Date().toISOString(),
          metadata:    JSON.stringify({ originalPaymentRef, reason }),
        },
      },
    }),
  ]);

  const fdiRes = await fdiRefund({
    trxID:  original.gatewayRef!,
    msisdn: phone,
    amount,
  });

  if (fdiRes.data?.gwRef) {
    await prisma.transaction.update({
      where: { paymentRef },
      data:  { gatewayRef: fdiRes.data.gwRef },
    });
  }

  await webhookQueue.add(
    'ttl-fallback',
    { paymentRef, provider: 'fdi' },
    { delay: 180_000, jobId: `ttl-${paymentRef}` },
  );
}
