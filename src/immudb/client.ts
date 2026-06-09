import { Client } from '@codenotary/immudb-node';
import { config } from '../config/env.js';

let immuClient: Client | null = null;
let isReconnecting = false;
let isShuttingDown = false;

const RETRY_DELAY_MS    = 5_000;
const KEEPALIVE_MS      = 30_000;

let keepaliveTimer: ReturnType<typeof setInterval> | null = null;

let immuHealth: { ok: boolean; error?: string } = { ok: false, error: 'not yet connected' };
export const getImmuDBHealth = () => immuHealth;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function isConnectionError(err: Error): boolean {
  const msg = err.message.toLowerCase();
  return (
    msg.includes('unavailable')    ||
    msg.includes('econnrefused')   ||
    msg.includes('etimedout')      ||
    msg.includes('not connected')  ||
    msg.includes('closed')         ||
    msg.includes('grpc')           ||
    msg.includes('transport')
  );
}

async function createClient(): Promise<Client> {
  const client = new Client({
    host:     config.immudb.host,
    port:     config.immudb.port,
    user:     config.immudb.user,
    password: config.immudb.password,
    database: config.immudb.database,
  });
  await client.sqlQuery({ sql: 'SELECT 1' });
  return client;
}

function scheduleReconnect(): void {
  if (isShuttingDown || isReconnecting) return;
  isReconnecting = true;
  immuHealth = { ok: false, error: 'reconnecting' };

  void (async () => {
    for (;;) {
      await sleep(RETRY_DELAY_MS);
      try {
        await immuClient?.close().catch(() => undefined);
        immuClient = null;
        immuClient = await createClient();
        immuHealth = { ok: true };
        isReconnecting = false;
        console.info('[immudb] Reconnected');
        return;
      } catch (err) {
        console.warn('[immudb] Reconnect failed:', (err as Error).message);
      }
    }
  })();
}

function startKeepalive(): void {
  if (keepaliveTimer) clearInterval(keepaliveTimer);
  keepaliveTimer = setInterval(async () => {
    if (isShuttingDown || isReconnecting || !immuClient) return;
    try {
      await immuClient.sqlQuery({ sql: 'SELECT 1' });
      immuHealth = { ok: true };
    } catch (err) {
      console.warn('[immudb] Keepalive failed — scheduling reconnect:', (err as Error).message);
      immuHealth = { ok: false, error: (err as Error).message };
      scheduleReconnect();
    }
  }, KEEPALIVE_MS);
}

export async function connectImmuDB(): Promise<void> {
  immuClient = await createClient();
  immuHealth = { ok: true };
  startKeepalive();
  console.info('[immudb] Connected to', config.immudb.host);
}

/**
 * Execute a function against the ImmuDB client.
 * On connection errors, triggers a reconnect and re-throws so the caller
 * (outbox worker) can back off and retry on the next poll cycle.
 */
export async function withImmuDB<T>(fn: (db: Client) => Promise<T>): Promise<T> {
  if (!immuClient) throw new Error('[immudb] Not connected');
  try {
    return await fn(immuClient);
  } catch (err) {
    if (isConnectionError(err as Error)) {
      immuHealth = { ok: false, error: (err as Error).message };
      scheduleReconnect();
    }
    throw err;
  }
}

export async function disconnectImmuDB(): Promise<void> {
  isShuttingDown = true;
  if (keepaliveTimer) { clearInterval(keepaliveTimer); keepaliveTimer = null; }
  if (immuClient) {
    await immuClient.close().catch(() => undefined);
    immuClient = null;
  }
}
