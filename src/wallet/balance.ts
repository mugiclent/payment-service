import { prisma } from '../db/prisma.js';
import { redis } from '../redis/client.js';
import { walletKey } from '../redis/walletDeduct.js';
import type { OwnerType } from '@prisma/client';

const WALLET_TTL = () => 3600 + Math.floor(Math.random() * 300);

/**
 * Returns current wallet balance. Redis is the primary source; falls back to
 * PostgreSQL on cache miss and seeds the cache for subsequent reads.
 */
export async function getBalance(ownerId: string): Promise<bigint> {
  try {
    const cached = await redis.get(walletKey(ownerId));
    if (cached !== null) return BigInt(cached);
  } catch (err) {
    console.warn('[wallet] Redis fallback to PostgreSQL:', (err as Error).message);
  }

  const wallet = await prisma.walletBalance.findFirst({
    where: { ownerId },
  });

  if (!wallet) throw new Error(`Wallet not found for owner ${ownerId}`);

  // Seed cache
  try {
    await redis.set(walletKey(ownerId), wallet.balance.toString(), 'EX', WALLET_TTL());
  } catch (err) {
    console.warn('[wallet] Failed to seed Redis cache:', (err as Error).message);
  }

  return wallet.balance;
}

export async function getWalletRecord(ownerId: string): Promise<{
  id: string;
  ownerId: string;
  ownerType: OwnerType;
  balance: bigint;
} | null> {
  return prisma.walletBalance.findFirst({ where: { ownerId } });
}

export async function seedCache(ownerId: string, balance: bigint): Promise<void> {
  try {
    await redis.set(walletKey(ownerId), balance.toString(), 'EX', WALLET_TTL());
  } catch (err) {
    console.warn('[wallet] Failed to seed cache:', (err as Error).message);
  }
}
