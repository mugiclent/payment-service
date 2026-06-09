import { prisma } from '../db/prisma.js';
import { fdiRefund } from '../gateway/fdi/client.js';
import { ttlQueue } from '../queues/webhookQueue.js';
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

  await prisma.transaction.create({
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
  });

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

  await ttlQueue.add(
    'ttl-fallback',
    { paymentRef, provider: 'fdi' },
    { delay: 180_000, jobId: `ttl-${paymentRef}` },
  );
}
