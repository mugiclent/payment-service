import { prisma } from '../db/prisma.js';
import { fdiPull } from '../gateway/fdi/client.js';
import { ttlQueue } from '../queues/webhookQueue.js';
import { config } from '../config/env.js';
import { inferMomoMethod, momoChannelId } from '../utils/phone.js';
import { assertUuid } from '../utils/validate.js';
import { publishTopupConfirmed, publishTopupFailed } from '../rabbitmq/publisher.js';
import { v4 as uuidv4 } from 'uuid';

export interface WalletTopupInput {
  topupId:  string;
  topupRef: string;
  userId:   string;
  phone:    string;
  amount:   bigint;
}

export async function handleWalletTopup(input: WalletTopupInput): Promise<void> {
  const { topupId, topupRef, userId, phone, amount } = input;

  // Validate all IDs are proper UUIDs
  try {
    assertUuid('topupRef',  topupRef);
    assertUuid('topupId',   topupId);
    assertUuid('userId',    userId);
  } catch (err) {
    publishTopupFailed({ topupId, topupRef, userId, amount, reason: (err as Error).message, failedAt: new Date().toISOString() });
    return;
  }

  // Idempotency — re-publish the outcome event so the caller gets the result
  const existing = await prisma.transaction.findUnique({ where: { paymentRef: topupRef } });
  if (existing) {
    if (existing.status === 'CONFIRMED') {
      const wallet = await prisma.walletBalance.findFirst({ where: { ownerId: userId } });
      publishTopupConfirmed({
        topupId:     existing.topupId ?? topupRef,
        topupRef,
        userId,
        amount:      existing.amount,
        newBalance:  wallet?.balance ?? BigInt(0),
        confirmedAt: existing.updatedAt.toISOString(),
      });
    } else if (existing.status === 'FAILED') {
      publishTopupFailed({
        topupId:  existing.topupId ?? topupRef,
        topupRef,
        userId,
        amount:   existing.amount,
        reason:   existing.failureReason ?? 'TOPUP_FAILED',
        failedAt: existing.updatedAt.toISOString(),
      });
    }
    // PENDING — still in flight, no event to send
    return;
  }

  // Validate phone network before writing anything to DB
  let method: ReturnType<typeof inferMomoMethod>;
  try {
    method = inferMomoMethod(phone);
  } catch (err) {
    publishTopupFailed({
      topupId,
      topupRef,
      userId,
      amount,
      reason:   (err as Error).message,
      failedAt: new Date().toISOString(),
    });
    return; // bad data — ack the message, no point retrying
  }

  const channelId = momoChannelId(method, config.fdi.mtnChannelId, config.fdi.airtelChannelId);

  // Wallet must exist before we can top it up
  const wallet = await prisma.walletBalance.findFirst({ where: { ownerId: userId, ownerType: 'PASSENGER' } });
  if (!wallet) {
    publishTopupFailed({ topupId, topupRef, userId, amount, reason: `Wallet not found for user ${userId}`, failedAt: new Date().toISOString() });
    return;
  }

  await prisma.transaction.create({
    data: {
      id:         uuidv4(),
      paymentRef: topupRef,
      topupId,
      userId,
      phone,
      type:       'WALLET_TOPUP',
      method,
      amount,
      currency:   'RWF',
      status:     'PENDING',
      provider:   'fdi',
    },
  });

  try {
    const fdiRes = await fdiPull({
      trxRef:    topupRef,
      channelId,
      msisdn:    phone,
      amount,
    });

    if (fdiRes.data?.gwRef) {
      await prisma.transaction.update({
        where: { paymentRef: topupRef },
        data:  { gatewayRef: fdiRes.data.gwRef },
      });
    }
  } catch (err) {
    const reason = (err as Error).message;
    await prisma.transaction.update({
      where: { paymentRef: topupRef },
      data:  { status: 'FAILED', failureReason: reason },
    });
    publishTopupFailed({
      topupId,
      topupRef,
      userId,
      amount,
      reason,
      failedAt: new Date().toISOString(),
    });
    throw err;
  }

  await ttlQueue.add(
    'ttl-fallback',
    { paymentRef: topupRef, topupId, provider: 'fdi' },
    { delay: 50_000, jobId: `ttl-${topupRef}` },
  );
}
