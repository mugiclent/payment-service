import { validateEnv, config } from './config/env.js';
import { prisma } from './db/prisma.js';
import { redis } from './redis/client.js';
import { setup as setupRabbitMQ, closeRabbitMQ, onReconnect } from './rabbitmq/client.js';
import { startConsumers, setShuttingDown } from './rabbitmq/consumer.js';
import { connectImmuDB, disconnectImmuDB } from './immudb/client.js';
import { initImmuDBSchema } from './immudb/schema.js';
import { startOutboxWorker, stopOutboxWorker } from './immudb/outboxWorker.js';
import { tokenManager } from './gateway/fdi/tokenManager.js';
import { fdiGetChannels } from './gateway/fdi/client.js';
import { startWebhookWorker } from './queues/webhookWorker.js';
import { startTtlWorker } from './queues/ttlWorker.js';
import { ttlQueue } from './queues/webhookQueue.js';
import { buildApp } from './app.js';
import type { Worker } from 'bullmq';

async function start(): Promise<void> {
  // ── Step 1: Validate env ──────────────────────────────────────────────────
  validateEnv();

  // ── Step 2: Connect PostgreSQL ────────────────────────────────────────────
  await prisma.$connect();
  console.info('[db] PostgreSQL connected');

  // ── Step 3: Run Prisma migrations ─────────────────────────────────────────
  // migrations are run by the deploy script before container starts

  // ── Step 4: Connect Redis ─────────────────────────────────────────────────
  await redis.ping();
  console.info('[redis] Ready');

  // ── Step 5: Connect RabbitMQ ──────────────────────────────────────────────
  await setupRabbitMQ();
  onReconnect(() => startConsumers());
  console.info('[rabbitmq] Connected');

  // ── Step 6: Connect ImmuDB ────────────────────────────────────────────────
  await connectImmuDB();

  // ── Step 7: Init ImmuDB schema ────────────────────────────────────────────
  await initImmuDBSchema();

  // ── Step 8: FDI token warmup ──────────────────────────────────────────────
  await tokenManager.warmup();

  // ── Step 9: Fetch FDI channels ────────────────────────────────────────────
  try {
    const channelsRes = await fdiGetChannels();
    const channels = channelsRes.data?.channels;
    if (!channels?.length) throw new Error('Empty or missing channels in FDI response');
    const mtnCh    = channels.find((c) => c.label.toLowerCase().includes('mtn'));
    const airtelCh = channels.find((c) => c.label.toLowerCase().includes('airtel'));
    if (mtnCh)    process.env['FDI_MTN_CHANNEL_ID']    = mtnCh.id;
    if (airtelCh) process.env['FDI_AIRTEL_CHANNEL_ID'] = airtelCh.id;
    console.info('[fdi] Channels:', channels.map((c) => `${c.id}(${c.state})`).join(', '));
    // Refresh config values now that env is updated
    config.fdi.mtnChannelId    = process.env['FDI_MTN_CHANNEL_ID']    ?? config.fdi.mtnChannelId;
    config.fdi.airtelChannelId = process.env['FDI_AIRTEL_CHANNEL_ID'] ?? config.fdi.airtelChannelId;
  } catch (err) {
    console.warn('[fdi] Failed to fetch channels:', (err as Error).message);
  }

  // ── Step 10: Start OutboxWorker ───────────────────────────────────────────
  await startOutboxWorker();

  // ── Step 11: Recover stale PENDING transactions ───────────────────────────
  // Any PENDING transaction older than 5 minutes was never resolved —
  // either the TTL job was lost or the service crashed. Re-queue immediately.
  const stale = await prisma.transaction.findMany({
    where: {
      status:    'PENDING',
      createdAt: { lt: new Date(Date.now() - 5 * 60 * 1000) },
    },
    select: { paymentRef: true, topupId: true },
  });
  if (stale.length) {
    console.warn(`[startup] Re-queuing ${stale.length} stale PENDING transaction(s)`);
    await Promise.all(stale.map((t) =>
      ttlQueue.add(
        'ttl-fallback',
        { paymentRef: t.paymentRef, topupId: t.topupId ?? undefined, provider: 'fdi' },
        { jobId: `ttl-${t.paymentRef}` },
      ).catch(() => undefined), // job already exists in queue — skip
    ));
  }

  // ── Step 12: Start BullMQ workers ────────────────────────────────────────
  const webhookWorker = startWebhookWorker();
  const ttlWorker     = startTtlWorker();

  // ── Step 12: Start RabbitMQ consumers ────────────────────────────────────
  try {
    await startConsumers();
  } catch (err) {
    console.warn('[rabbitmq] Failed to start consumers:', (err as Error).message);
    setTimeout(() => startConsumers().catch(console.warn), 5_000);
  }

  // ── Step 13: Start Fastify ────────────────────────────────────────────────
  const app = buildApp();
  await app.listen({ port: config.port, host: '0.0.0.0' });
  console.info(`[http] Listening on port ${config.port}`);

  // ── Step 14: Ready ────────────────────────────────────────────────────────
  console.info('[payment-service] Payment service ready');

  // ── Graceful shutdown ─────────────────────────────────────────────────────
  const shutdown = async (): Promise<void> => {
    console.info('[shutdown] SIGTERM received — draining...');
    setTimeout(() => process.exit(1), 35_000).unref();

    setShuttingDown();
    stopOutboxWorker();
    tokenManager.stop();

    const drain = (w: Worker) =>
      Promise.race([w.close(), new Promise((r) => setTimeout(r, 30_000))]);

    await Promise.all([drain(webhookWorker), drain(ttlWorker)]);

    await app.close();
    await closeRabbitMQ();
    await prisma.$disconnect();
    await redis.quit();
    await disconnectImmuDB();

    console.info('[shutdown] Clean exit');
    setTimeout(() => process.exit(0), 500);
  };

  process.once('SIGTERM', () => void shutdown());
  process.once('SIGINT',  () => void shutdown());
}

start().catch((err) => {
  console.error('[startup] Fatal error:', err);
  process.exit(1);
});
