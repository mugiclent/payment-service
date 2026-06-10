import { prisma } from '../db/prisma.js';
import { withImmuDB } from './client.js';
import { v4 as uuidv4 } from 'uuid';
import Long from 'long';

const MAX_ATTEMPTS = 10;

function isPermanentError(err: Error): boolean {
  return err.message.includes('max length exceeded') ||
         err.message.includes('unique constraint') ||
         err.message.includes('not null constraint');
}

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
  const p  = entry.payload as Record<string, unknown>;

  const IMMUDB_EVENT_TYPES = new Set([
    'WALLET_CREDIT',
    'WALLET_DEBIT',
    'PLATFORM_FEE',
    'OPERATOR_PAYOUT_PENDING',
  ]);

  if (!IMMUDB_EVENT_TYPES.has(entry.eventType)) return;

  const varchar = (name: string, val: unknown): { name: string; type: 'VARCHAR'; value: string } =>
    ({ name, type: 'VARCHAR', value: String(val) });

  const nullOrVarchar = (name: string, val: unknown) =>
    val != null
      ? ({ name, type: 'VARCHAR' as const, value: String(val) })
      : ({ name, type: 'NULL'    as const });

  await withImmuDB((db) => db.sqlExec({
    sql: `INSERT INTO payment_events (
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
    params: [
      varchar('id',          p['id']          ?? entry.id),
      varchar('event_type',  p['event_type']  ?? entry.eventType),
      varchar('payment_ref', p['payment_ref'] ?? entry.paymentRef),
      varchar('owner_id',    p['owner_id']    ?? ''),
      varchar('owner_type',  p['owner_type']  ?? ''),
      { name: 'amount', type: 'INTEGER', value: Long.fromNumber(Number(p['amount'] ?? 0)) },
      varchar('currency',    p['currency']    ?? 'RWF'),
      nullOrVarchar('method',      p['method']),
      varchar('status',      p['status']      ?? ''),
      nullOrVarchar('ticket_id',   p['ticket_id']),
      nullOrVarchar('trip_id',     p['trip_id']),
      nullOrVarchar('org_id',      p['org_id']),
      nullOrVarchar('gateway_ref', p['gateway_ref']),
      varchar('occurred_at', p['occurred_at'] ?? new Date().toISOString()),
      nullOrVarchar('metadata',    p['metadata']),
    ],
  }));
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
      const error = err as Error;
      console.error(
        `[outbox] Failed to write entry ${entry.id} (attempt ${entry.attempts + 1}):`,
        error.message,
      );

      if (isPermanentError(error) || entry.attempts + 1 >= MAX_ATTEMPTS) {
        // Permanent error or retry cap — fix bad paymentRef with a valid UUID
        // so the entry can be written to ImmuDB on the next cycle.
        const fixedRef     = uuidv4();
        const fixedPayload = { ...(entry.payload as Record<string, unknown>), payment_ref: fixedRef };
        console.warn(`[outbox] Permanent error for ${entry.id}, repairing paymentRef → ${fixedRef}`);
        await prisma.outboxEntry.update({
          where: { id: entry.id },
          data: {
            paymentRef:    fixedRef,
            payload:       fixedPayload,
            attempts:      { increment: 1 },
            lastAttemptAt: new Date(),
            error:         `[REPAIRED] ${error.message.slice(0, 480)}`,
          },
        }).catch(() => undefined);
      } else {
        const delay = Math.min(5_000 * 2 ** entry.attempts, MAX_BACKOFF_MS);
        await prisma.outboxEntry.update({
          where: { id: entry.id },
          data: {
            attempts:      { increment: 1 },
            lastAttemptAt: new Date(),
            error:         error.message.slice(0, 500),
          },
        }).catch(() => undefined);
        await sleep(delay);
      }
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
