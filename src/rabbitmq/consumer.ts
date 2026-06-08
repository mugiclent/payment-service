import type { Channel } from 'amqplib';
import { getConnection } from './client.js';
import { handleWalletPayment } from '../handlers/walletPayment.js';
import { handleMomoPayment } from '../handlers/momoPayment.js';
import { handleCashPayment } from '../handlers/cashPayment.js';
import { handleWalletRefund } from '../handlers/walletRefund.js';
import { handleMomoRefund } from '../handlers/momoRefund.js';
import { createWallet } from '../wallet/creation.js';

const DLX = 'payment.dlx';

let isShuttingDown        = false;
let isReconnecting        = false;
let isReconnectingChannel = false;

const RETRY_DELAY_MS = 3_000;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function bigintFromMsg(val: unknown): bigint {
  return BigInt(String(val));
}

function parse(content: Buffer): unknown {
  return JSON.parse(content.toString('utf8'));
}

async function setupConsumerChannels(): Promise<void> {
  const conn = getConnection();

  // ── payment.requested ───────────────────────────────────────────────────────
  const requestCh: Channel = await conn.createChannel();
  await requestCh.prefetch(1);
  await requestCh.checkExchange('payment');
  await requestCh.checkExchange(DLX);

  await requestCh.assertQueue('payment-svc.requests', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLX,
      'x-message-ttl':         180_000,
    },
  });
  await requestCh.bindQueue('payment-svc.requests', 'payment', 'payment.requested');

  requestCh.on('error', (err: Error) =>
    console.warn('[rabbitmq] requestCh error:', err.message),
  );
  requestCh.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    setTimeout(() => {
      void setupConsumerChannels()
        .catch((e: Error) => console.warn('[rabbitmq] Failed to re-create consumer channels:', e.message))
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  await requestCh.consume('payment-svc.requests', async (msg) => {
    if (!msg) return;
    try {
      const payload = parse(msg.content) as Record<string, unknown>;
      const method  = payload['method'] as string;

      if (method === 'wallet') {
        await handleWalletPayment({
          paymentRef: String(payload['paymentRef']),
          userId:     String(payload['userId']),
          amount:     bigintFromMsg(payload['amount']),
          currency:   String(payload['currency'] ?? 'RWF'),
          ticketId:   payload['ticketId'] as string | null,
          tripId:     payload['tripId']   as string | null,
          orgId:      payload['orgId']    as string | null,
        });
      } else if (method === 'mtn' || method === 'airtel') {
        await handleMomoPayment({
          paymentRef: String(payload['paymentRef']),
          method,
          phone:      String(payload['phone']),
          amount:     bigintFromMsg(payload['amount']),
          currency:   String(payload['currency'] ?? 'RWF'),
          userId:     payload['userId']   as string | null,
          ticketId:   payload['ticketId'] as string | null,
          tripId:     payload['tripId']   as string | null,
          orgId:      payload['orgId']    as string | null,
        });
      } else if (method === 'cash') {
        await handleCashPayment({
          paymentRef: String(payload['paymentRef']),
          orgId:      String(payload['orgId']),
          amount:     bigintFromMsg(payload['amount']),
          currency:   String(payload['currency'] ?? 'RWF'),
          userId:     payload['userId']   as string | null,
          ticketId:   payload['ticketId'] as string | null,
          tripId:     payload['tripId']   as string | null,
        });
      } else {
        console.warn('[consumer] Unknown payment method:', method);
      }

      try { requestCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] payment.requested failed:', (err as Error).message);
      try { requestCh.nack(msg, false, false); } catch { /* channel closed */ }
    }
  });

  // ── refund.requested ────────────────────────────────────────────────────────
  const refundCh: Channel = await conn.createChannel();
  await refundCh.prefetch(1);
  await refundCh.checkExchange('payment');

  await refundCh.assertQueue('payment-svc.refunds', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  await refundCh.bindQueue('payment-svc.refunds', 'payment', 'refund.requested');

  refundCh.on('error', (err: Error) =>
    console.warn('[rabbitmq] refundCh error:', err.message),
  );
  refundCh.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    setTimeout(() => {
      void setupConsumerChannels()
        .catch((e: Error) => console.warn('[rabbitmq] Failed to re-create consumer channels:', e.message))
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  await refundCh.consume('payment-svc.refunds', async (msg) => {
    if (!msg) return;
    try {
      const payload = parse(msg.content) as Record<string, unknown>;

      // Determine refund type from original transaction
      const originalMethod = payload['originalMethod'] as string | undefined;

      if (originalMethod === 'wallet') {
        await handleWalletRefund({
          paymentRef:         String(payload['paymentRef']),
          originalPaymentRef: String(payload['originalPaymentRef']),
          userId:             String(payload['userId']),
          amount:             bigintFromMsg(payload['amount']),
          currency:           String(payload['currency'] ?? 'RWF'),
          ticketId:           payload['ticketId'] as string | null,
          reason:             payload['reason'] as string | undefined,
        });
      } else {
        await handleMomoRefund({
          paymentRef:         String(payload['paymentRef']),
          originalPaymentRef: String(payload['originalPaymentRef']),
          phone:              String(payload['phone']),
          userId:             payload['userId'] as string | null,
          amount:             bigintFromMsg(payload['amount']),
          currency:           String(payload['currency'] ?? 'RWF'),
          ticketId:           payload['ticketId'] as string | null,
          reason:             payload['reason'] as string | undefined,
        });
      }

      try { refundCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] refund.requested failed:', (err as Error).message);
      try { refundCh.nack(msg, false, false); } catch { /* channel closed */ }
    }
  });

  // ── user.passenger.created ───────────────────────────────────────────────────
  const usersCh: Channel = await conn.createChannel();
  await usersCh.prefetch(1);
  await usersCh.checkExchange('users');

  await usersCh.assertQueue('users-payment-svc', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  await usersCh.bindQueue('users-payment-svc', 'users', 'user.passenger.created');

  usersCh.on('error', (err: Error) =>
    console.warn('[rabbitmq] usersCh error:', err.message),
  );
  usersCh.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    setTimeout(() => {
      void setupConsumerChannels()
        .catch((e: Error) => console.warn('[rabbitmq] Failed to re-create consumer channels:', e.message))
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  await usersCh.consume('users-payment-svc', async (msg) => {
    if (!msg) return;
    try {
      const payload = parse(msg.content) as Record<string, unknown>;
      await createWallet(String(payload['userId']), 'PASSENGER');
      try { usersCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] user.passenger.created failed:', (err as Error).message);
      try { usersCh.nack(msg, false, false); } catch { /* channel closed */ }
    }
  });

  // ── org.activated ─────────────────────────────────────────────────────────
  const billingCh: Channel = await conn.createChannel();
  await billingCh.prefetch(1);
  await billingCh.checkExchange('billing');

  await billingCh.assertQueue('billing-payment-svc', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  await billingCh.bindQueue('billing-payment-svc', 'billing', 'org.activated');

  billingCh.on('error', (err: Error) =>
    console.warn('[rabbitmq] billingCh error:', err.message),
  );
  billingCh.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    setTimeout(() => {
      void setupConsumerChannels()
        .catch((e: Error) => console.warn('[rabbitmq] Failed to re-create consumer channels:', e.message))
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  await billingCh.consume('billing-payment-svc', async (msg) => {
    if (!msg) return;
    try {
      const payload = parse(msg.content) as Record<string, unknown>;
      await createWallet(String(payload['orgId']), 'ORGANISATION');
      try { billingCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] org.activated failed:', (err as Error).message);
      try { billingCh.nack(msg, false, false); } catch { /* channel closed */ }
    }
  });

  console.info('[rabbitmq] All consumer channels active');
}

export async function startConsumers(): Promise<void> {
  await setupConsumerChannels();
}

export function setShuttingDown(): void {
  isShuttingDown = true;
}
