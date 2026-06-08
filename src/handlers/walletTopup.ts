import { prisma } from '../db/prisma.js';
import { fdiPull } from '../gateway/fdi/client.js';
import { webhookQueue } from '../queues/webhookQueue.js';
import { config } from '../config/env.js';
import { v4 as uuidv4 } from 'uuid';

export interface WalletTopupInput {
  topupRef: string;
  userId:   string;
  phone:    string;
  amount:   bigint;
}

export async function handleWalletTopup(input: WalletTopupInput): Promise<{ status: number }> {
  const { topupRef, userId, phone, amount } = input;

  const existing = await prisma.transaction.findUnique({ where: { paymentRef: topupRef } });
  if (existing) return { status: 202 };

  await prisma.$transaction([
    prisma.transaction.create({
      data: {
        id:         uuidv4(),
        paymentRef: topupRef,
        userId,
        phone,
        type:       'WALLET_TOPUP',
        method:     'mtn',
        amount,
        currency:   'RWF',
        status:     'PENDING',
        provider:   'fdi',
      },
    }),
    prisma.outboxEntry.create({
      data: {
        id:         uuidv4(),
        eventType:  'TOPUP_REQUESTED',
        paymentRef: topupRef,
        payload: {
          id:          uuidv4(),
          event_type:  'TOPUP_REQUESTED',
          payment_ref: topupRef,
          owner_id:    userId,
          owner_type:  'PASSENGER',
          amount:      Number(amount),
          currency:    'RWF',
          method:      'mtn',
          status:      'PENDING',
          ticket_id:   null,
          trip_id:     null,
          org_id:      null,
          gateway_ref: null,
          occurred_at: new Date().toISOString(),
          metadata:    null,
        },
      },
    }),
  ]);

  const fdiRes = await fdiPull({
    trxRef:    topupRef,
    channelId: config.fdi.mtnChannelId,
    msisdn:    phone,
    amount,
  });

  if (fdiRes.data?.gwRef) {
    await prisma.transaction.update({
      where: { paymentRef: topupRef },
      data:  { gatewayRef: fdiRes.data.gwRef },
    });
  }

  await webhookQueue.add(
    'ttl-fallback',
    { paymentRef: topupRef, provider: 'fdi' },
    { delay: 180_000, jobId: `ttl:${topupRef}` },
  );

  return { status: 202 };
}
