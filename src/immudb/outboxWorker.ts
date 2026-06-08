import { prisma } from '../db/prisma.js';
import { getImmuDB } from './client.js';

const POLL_INTERVAL_MS = 2_000;
const MAX_BACKOFF_MS   = 60_000;

let isRunning = false;
let stopRequested = false;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function processEntry(entry: {
  id: string;
  eventType: string;
  paymentRef: string;
  payload: unknown;
  attempts: number;
}): Promise<void> {
  const db = getImmuDB();
  const p  = entry.payload as Record<string, unknown>;

  await db.exec(
    `INSERT INTO payment_events (
       id, event_type, payment_ref, owner_id, owner_type,
       amount, currency, method, status,
       ticket_id, trip_id, org_id, gateway_ref,
       occurred_at, metadata
     ) VALUES (
       @id, @event_type, @payment_ref, @owner_id, @owner_type,
       @amount, @currency, @method, @status,
       @ticket_id, @trip_id, @org_id, @gateway_ref,
       @occurred_at, @metadata
     )`,
    {
      id:          String(p['id']          ?? entry.id),
      event_type:  String(p['event_type']  ?? entry.eventType),
      payment_ref: String(p['payment_ref'] ?? entry.paymentRef),
      owner_id:    String(p['owner_id']    ?? ''),
      owner_type:  String(p['owner_type']  ?? ''),
      amount:      Number(p['amount']      ?? 0),
      currency:    String(p['currency']    ?? 'RWF'),
      method:      p['method']      != null ? String(p['method'])      : null,
      status:      String(p['status']      ?? ''),
      ticket_id:   p['ticket_id']   != null ? String(p['ticket_id'])   : null,
      trip_id:     p['trip_id']     != null ? String(p['trip_id'])     : null,
      org_id:      p['org_id']      != null ? String(p['org_id'])      : null,
      gateway_ref: p['gateway_ref'] != null ? String(p['gateway_ref']) : null,
      occurred_at: String(p['occurred_at'] ?? new Date().toISOString()),
      metadata:    p['metadata']    != null ? String(p['metadata'])    : null,
    },
  );
}

async function runOnce(): Promise<void> {
  const pending = await prisma.outboxEntry.findMany({
    where:   { processedAt: null },
    orderBy: { createdAt: 'asc' },
    take:    100,
  });

  for (const entry of pending) {
    if (stopRequested) break;

    try {
      await processEntry(entry);
      await prisma.outboxEntry.update({
        where: { id: entry.id },
        data:  { processedAt: new Date() },
      });
    } catch (err) {
      const delay = Math.min(5_000 * 2 ** entry.attempts, MAX_BACKOFF_MS);
      console.error(
        `[outbox] Failed to write entry ${entry.id} (attempt ${entry.attempts + 1}):`,
        (err as Error).message,
      );
      await prisma.outboxEntry.update({
        where: { id: entry.id },
        data: {
          attempts:      { increment: 1 },
          lastAttemptAt: new Date(),
          error:         (err as Error).message.slice(0, 500),
        },
      }).catch(() => undefined);
      await sleep(delay);
    }
  }
}

export async function startOutboxWorker(): Promise<void> {
  if (isRunning) return;
  isRunning = true;
  stopRequested = false;

  console.info('[outbox] Worker started');

  void (async () => {
    while (!stopRequested) {
      try {
        await runOnce();
      } catch (err) {
        console.error('[outbox] Unexpected error:', (err as Error).message);
      }
      await sleep(POLL_INTERVAL_MS);
    }
    isRunning = false;
    console.info('[outbox] Worker stopped');
  })();
}

export function stopOutboxWorker(): void {
  stopRequested = true;
}
