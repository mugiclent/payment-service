import { prisma } from '../db/prisma.js';
import { fdiPull } from '../gateway/fdi/client.js';
import { ttlQueue } from '../queues/webhookQueue.js';
import { config } from '../config/env.js';
import { inferMomoMethod, momoChannelId } from '../utils/phone.js';
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

  const existing = await prisma.transaction.findUnique({ where: { paymentRef: topupRef } });
  if (existing) return;

  const method    = inferMomoMethod(phone);
  const channelId = momoChannelId(method, config.fdi.mtnChannelId, config.fdi.airtelChannelId);

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

  const fdiRes = await fdiPull({
    trxRef: topupRef,
    channelId,
    msisdn: phone,
    amount,
  });

  if (fdiRes.data?.gwRef) {
    await prisma.transaction.update({
      where: { paymentRef: topupRef },
      data:  { gatewayRef: fdiRes.data.gwRef },
    });
  }

  await ttlQueue.add(
    'ttl-fallback',
    { paymentRef: topupRef, topupId, provider: 'fdi' },
    { delay: 180_000, jobId: `ttl-${topupRef}` },
  );
}
