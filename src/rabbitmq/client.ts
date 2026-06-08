import amqplib from 'amqplib';
import type { Channel, ChannelModel } from 'amqplib';
import { config } from '../config/env.js';

const RETRY_DELAY_MS = 3_000;
const MAX_BUFFER = 500;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

let connection: ChannelModel;
let publishChannel: Channel;
let isShuttingDown      = false;
let isReconnecting      = false;
let isReconnectingChannel = false;

type RabbitHealth = { ok: boolean; error?: string };
let rabbitHealth: RabbitHealth = { ok: false, error: 'not yet connected' };
export const getRabbitMQHealth = (): RabbitHealth => rabbitHealth;

// In-memory publish buffer used during reconnection
const publishBuffer: Array<{ exchange: string; key: string; content: Buffer; opts: object }> = [];

// Callbacks invoked after a successful reconnect (e.g., to re-create consumer channels)
const reconnectHooks: Array<() => Promise<void>> = [];
export function onReconnect(fn: () => Promise<void>): void {
  reconnectHooks.push(fn);
}

export async function setupChannels(): Promise<void> {
  publishChannel = await connection.createChannel();
  await publishChannel.prefetch(1);

  publishChannel.on('error', (err: Error) =>
    console.warn('[rabbitmq] publishChannel error:', err.message),
  );
  publishChannel.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    rabbitHealth = { ok: false, error: 'publishChannel closed — re-creating' };
    setTimeout(() => {
      void setupChannels()
        .catch((err: Error) =>
          console.warn('[rabbitmq] Failed to re-create publish channel:', err.message),
        )
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  rabbitHealth = { ok: true };
  console.info('[rabbitmq] Publish channel ready');

  // Drain buffer accumulated during downtime
  while (publishBuffer.length > 0) {
    const msg = publishBuffer.shift()!;
    try {
      publishChannel.publish(msg.exchange, msg.key, msg.content, msg.opts as amqplib.Options.Publish);
    } catch {
      // channel still broken — put it back and stop draining
      publishBuffer.unshift(msg);
      break;
    }
  }
}

export async function setup(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      connection = await amqplib.connect(config.rabbitmq.url);
      break;
    } catch (err) {
      console.warn(
        `[rabbitmq] Connection attempt ${attempt} failed:`,
        (err as Error).message,
      );
      await sleep(Math.min(1000 * 2 ** (attempt - 1), 30_000));
    }
  }

  connection.on('close', scheduleReconnect);
  connection.on('error', (err: Error) =>
    console.warn('[rabbitmq] Connection error:', err.message),
  );

  await setupChannels();
}

export function scheduleReconnect(): void {
  if (isShuttingDown || isReconnecting) return;
  isReconnecting = true;
  void (async () => {
    for (;;) {
      await sleep(RETRY_DELAY_MS);
      try {
        await setup();
        isReconnecting = false;
        // Re-create consumer channels after connection is restored
        for (const hook of reconnectHooks) {
          await hook().catch((e: Error) =>
            console.warn('[rabbitmq] Reconnect hook failed:', e.message),
          );
        }
        return;
      } catch (err) {
        console.warn('[rabbitmq] Reconnect failed:', (err as Error).message);
        try { await connection?.close(); } catch { /* already closed */ }
      }
    }
  })();
}

export function publish(
  exchange: string,
  routingKey: string,
  payload: object,
): void {
  const content = Buffer.from(
    JSON.stringify(payload, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
  );
  const opts = { deliveryMode: 2, persistent: true };

  try {
    publishChannel.publish(exchange, routingKey, content, opts);
  } catch {
    // Channel not ready — buffer for drain after reconnect
    if (publishBuffer.length < MAX_BUFFER) {
      publishBuffer.push({ exchange, key: routingKey, content, opts });
    } else {
      console.error('[rabbitmq] Publish buffer full — dropping message', routingKey);
    }
  }
}

export function getConnection(): ChannelModel {
  return connection;
}

export async function closeRabbitMQ(): Promise<void> {
  isShuttingDown = true;
  try { await publishChannel?.close(); } catch { /* ok */ }
  try { await connection?.close(); } catch { /* ok */ }
}
