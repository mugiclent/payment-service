import { prisma } from '../db/prisma.js';
import { fdiPull } from '../gateway/fdi/client.js';
import { publishPaymentConfirmed, publishPaymentFailed } from '../rabbitmq/publisher.js';
import { ttlQueue } from '../queues/webhookQueue.js';
import { config } from '../config/env.js';
import { assertUuid, assertUuidIfPresent } from '../utils/validate.js';
import { computeGatewayMarkup } from '../payments/fee.js';
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

  try {
    assertUuid('paymentRef', paymentRef);
    assertUuidIfPresent('userId',   userId);
    assertUuidIfPresent('orgId',    orgId);
    assertUuidIfPresent('ticketId', ticketId);
    assertUuidIfPresent('tripId',   tripId);
  } catch (err) {
    publishPaymentFailed({ paymentRef, method, amount, userId, phone, ticketId, reason: (err as Error).message, failedAt: new Date().toISOString(), retryable: false });
    return;
  }

  const existing = await prisma.transaction.findUnique({ where: { paymentRef } });
  if (existing) {
    if (existing.status === 'CONFIRMED') {
      publishPaymentConfirmed({
        paymentRef,
        method:      existing.method,
        amount:      existing.amount,
        currency:    existing.currency,
        userId:      existing.userId,
        phone:       existing.phone,
        ticketId:    existing.ticketId,
        tripId:      existing.tripId,
        orgId:       existing.orgId,
        confirmedAt: existing.updatedAt.toISOString(),
        gatewayRef:  existing.gatewayRef,
        feeAmount:   existing.feeAmount,
        netAmount:   existing.feeAmount != null ? existing.amount - existing.feeAmount : null,
      });
    } else if (existing.status === 'FAILED') {
      publishPaymentFailed({
        paymentRef,
        method:    existing.method,
        amount:    existing.amount,
        userId:    existing.userId,
        phone:     existing.phone,
        ticketId:  existing.ticketId,
        reason:    existing.failureReason ?? 'PAYMENT_FAILED',
        failedAt:  existing.updatedAt.toISOString(),
        retryable: false,
      });
    }
    // PENDING — still in flight
    return;
  }

  await prisma.transaction.create({
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
  });

  const channelId =
    method === 'mtn' ? config.fdi.mtnChannelId : config.fdi.airtelChannelId;

  try {
    const fdiRes = await fdiPull({ trxRef: paymentRef, channelId, msisdn: phone, amount: amount + computeGatewayMarkup(amount) });

    if (fdiRes.data?.gwRef) {
      await prisma.transaction.update({
        where: { paymentRef },
        data:  { gatewayRef: fdiRes.data.gwRef },
      });
    }

    // Schedule TTL fallback — fires after 3 minutes if webhook never arrives
    await ttlQueue.add(
      'ttl-fallback',
      { paymentRef, provider: 'fdi' },
      { delay: 90_000, jobId: `ttl-${paymentRef}` },
    );
  } catch (err) {
    const error = err as Error;
    const retryable = !error.message.includes('400') && !error.message.includes('422');

    await prisma.transaction.update({
      where: { paymentRef },
      data:  { status: 'FAILED', failureReason: error.message },
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
