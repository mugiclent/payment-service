import type { FastifyInstance } from 'fastify';
import { prisma } from '../db/prisma.js';
import { redis } from '../redis/client.js';
import { getRabbitMQHealth } from '../rabbitmq/client.js';
import { getImmuDBHealth } from '../immudb/client.js';
import { tokenManager } from '../gateway/fdi/tokenManager.js';

let dbHealthCache: { ok: boolean; error?: string; ts: number } | null = null;

async function checkDbHealth(): Promise<{ ok: boolean; error?: string; ts: number }> {
  const now = Date.now();
  if (dbHealthCache && now - dbHealthCache.ts < 5_000) return dbHealthCache;
  try {
    await prisma.$queryRaw`SELECT 1`;
    return (dbHealthCache = { ok: true, ts: now });
  } catch (err) {
    return (dbHealthCache = { ok: false, error: (err as Error).message, ts: now });
  }
}

async function checkRedisHealth(): Promise<{ ok: boolean; error?: string }> {
  try {
    await redis.ping();
    return { ok: true };
  } catch (err) {
    return { ok: false, error: (err as Error).message };
  }
}

function checkImmuDBHealth(): { ok: boolean; error?: string } {
  return getImmuDBHealth();
}

async function checkFdiToken(): Promise<{ ok: boolean }> {
  try {
    await tokenManager.getToken();
    return { ok: true };
  } catch {
    return { ok: false };
  }
}

export async function healthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/health', async (_req, reply) => {
    const immudb = checkImmuDBHealth();
    const [db, redisHealth, fdiToken, outboxPending] = await Promise.all([
      checkDbHealth(),
      checkRedisHealth(),
      checkFdiToken(),
      prisma.outboxEntry.count({ where: { processedAt: null } }).catch(() => -1),
    ]);

    const rabbit = getRabbitMQHealth();

    const degraded = !db.ok || !redisHealth.ok || !rabbit.ok || !immudb.ok;
    const status = degraded ? 503 : 200;

    return reply.status(status).send({
      status:        degraded ? 'degraded' : 'ok',
      service:       'payment-service',
      timestamp:     new Date().toISOString(),
      postgres:      db.ok,
      redis:         redisHealth.ok,
      rabbitmq:      rabbit.ok,
      immudb:        immudb.ok,
      fdiToken:      fdiToken.ok,
      outboxPending,
      checks: {
        database: db.ok
          ? 'up'
          : { status: 'down', error: db.error },
        rabbitmq: rabbit.ok
          ? 'up'
          : { status: 'down', error: rabbit.error },
      },
    });
  });
}
