import { prisma } from '../db/prisma.js';
import { fdiPull } from '../gateway/fdi/client.js';
import { publishPaymentFailed } from '../rabbitmq/publisher.js';
import { webhookQueue } from '../queues/webhookQueue.js';
import { config } from '../config/env.js';
import { v4 as uuidv4 } from 'uuid';

export interface MomoPaymentInput {
  paymentRef: string;
  method:     'mtn' | 'airtel';
  phone:      string;
  amount:     bigint;
  currency:   string;
  userId?:    string | null;
  ticketId?:  string | null;
  tripId?:    string | null;
  orgId?:     string | null;
}

export async function handleMomoPayment(input: MomoPaymentInput): Promise<void> {
  const { paymentRef, method, phone, amount, currency, userId, ticketId, tripId, orgId } = input;

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) return;

  const eventType = method === 'mtn' ? 'MTN_PAYMENT_REQUESTED' : 'AIRTEL_PAYMENT_REQUESTED';

  await prisma.$transaction([
    prisma.transaction.create({
      data: {
        id:       uuidv4(),
        paymentRef,
        userId,
        phone,
        type:     'TICKET_PAYMENT',
        method,
        amount,
        currency,
        status:   'PENDING',
        provider: 'fdi',
        ticketId,
        tripId,
        orgId,
      },
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType,
        paymentRef,
        payload: {
          id:          uuidv4(),
          event_type:  eventType,
          payment_ref: paymentRef,
          owner_id:    userId ?? phone,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency,
          method,
          status:      'PENDING',
          ticket_id:   ticketId ?? null,
          trip_id:     tripId ?? null,
          org_id:      orgId ?? null,
          gateway_ref: null,
          occurred_at: new Date().toISOString(),
          metadata:    null,
        },
      },
    }),
  ]);

  const channelId =
    method === 'mtn' ? config.fdi.mtnChannelId : config.fdi.airtelChannelId;

  try {
    const fdiRes = await fdiPull({ trxRef: paymentRef, channelId, msisdn: phone, amount });

    if (fdiRes.data?.gwRef) {
      await prisma.transaction.update({
        where: { paymentRef },
        data:  { gatewayRef: fdiRes.data.gwRef },
      });
    }

    // Schedule TTL fallback — fires after 3 minutes if webhook never arrives
    await webhookQueue.add(
      'ttl-fallback',
      { paymentRef, provider: 'fdi' },
      { delay: 180_000, jobId: `ttl:${paymentRef}` },
    );
  } catch (err) {
    const error = err as Error;
    const retryable = !error.message.includes('400') && !error.message.includes('422');

    await prisma.transaction.update({
      where: { paymentRef },
      data:  { status: 'FAILED' },
    });

    publishPaymentFailed({
      paymentRef,
      method,
      amount,
      userId,
      phone,
      ticketId,
      reason:    error.message,
      failedAt:  new Date().toISOString(),
      retryable,
    });
  }
}
