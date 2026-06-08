import { prisma } from '../db/prisma.js';
import { redis } from '../redis/client.js';
import { walletKey } from '../redis/walletDeduct.js';
import type { OwnerType } from '@prisma/client';

export async function createWallet(ownerId: string, ownerType: OwnerType): Promise<void> {
  await prisma.walletBalance.upsert({
    where: { ownerId_ownerType: { ownerId, ownerType } },
    create: { ownerId, ownerType, balance: 0n },
    update: {},
  });

  await redis.set(walletKey(ownerId), '0', 'EX', 3600);

  console.info(`[wallet] Created ${ownerType} wallet for ${ownerId}`);
}
