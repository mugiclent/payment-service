import type { Channel } from 'amqplib';
import { getConnection } from './client.js';
import { handleWalletPayment } from '../handlers/walletPayment.js';
import { handleMomoPayment } from '../handlers/momoPayment.js';
import { handleCashPayment } from '../handlers/cashPayment.js';
import { handleWalletRefund } from '../handlers/walletRefund.js';
import { handleMomoRefund } from '../handlers/momoRefund.js';
import { createWallet } from '../wallet/creation.js';
import { handleWalletTopup } from '../handlers/walletTopup.js';

const TRIPS_DLX = 'payment.dlx';
const USERS_DLX  = 'users.dlx';

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

  // ── trips-payment-svc ────────────────────────────────────────────────────
  // Consumes payment.requested and refund.requested from the trips exchange.
  // trips is owned by trip-service (not broker-predefined) so we assertExchange.
  const tripsCh: Channel = await conn.createChannel();
  await tripsCh.prefetch(1);
  await tripsCh.assertExchange('trips', 'topic', { durable: true });
  await tripsCh.checkExchange(TRIPS_DLX);

  await tripsCh.assertQueue('trips-payment-svc', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': TRIPS_DLX },
  });
  await tripsCh.bindQueue('trips-payment-svc', 'trips', 'payment.requested');
  await tripsCh.bindQueue('trips-payment-svc', 'trips', 'refund.requested');

  tripsCh.on('error', (err: Error) =>
    console.warn('[rabbitmq] tripsCh error:', err.message),
  );
  tripsCh.on('close', () => {
    if (isShuttingDown || isReconnecting || isReconnectingChannel) return;
    isReconnectingChannel = true;
    setTimeout(() => {
      void setupConsumerChannels()
        .catch((e: Error) => console.warn('[rabbitmq] Failed to re-create consumer channels:', e.message))
        .finally(() => { isReconnectingChannel = false; });
    }, RETRY_DELAY_MS);
  });

  await tripsCh.consume('trips-payment-svc', async (msg) => {
    if (!msg) return;
    try {
      const payload = parse(msg.content) as Record<string, unknown>;
      const type    = payload['type'] as string;

      if (type === 'payment.requested') {
        const method = payload['payment_method'] as string;
        if (method === 'wallet') {
          await handleWalletPayment({
            paymentRef: String(payload['payment_ref']),
            userId:     String(payload['user_id']),
            amount:     bigintFromMsg(payload['ticket_price']),
            currency:   'RWF',
            ticketId:   payload['ticket_id'] as string | null,
            tripId:     payload['trip_id']   as string | null,
            orgId:      payload['org_id']    as string | null,
          });
        } else if (method === 'mtn' || method === 'airtel') {
          await handleMomoPayment({
            paymentRef: String(payload['payment_ref']),
            method,
            phone:      String(payload['phone']),
            amount:     bigintFromMsg(payload['ticket_price']),
            currency:   'RWF',
            userId:     payload['user_id']   as string | null,
            ticketId:   payload['ticket_id'] as string | null,
            tripId:     payload['trip_id']   as string | null,
            orgId:      payload['org_id']    as string | null,
          });
        } else if (method === 'cash') {
          await handleCashPayment({
            paymentRef: String(payload['payment_ref']),
            orgId:      String(payload['org_id']),
            amount:     bigintFromMsg(payload['ticket_price']),
            currency:   'RWF',
            userId:     payload['user_id']   as string | null,
            ticketId:   payload['ticket_id'] as string | null,
            tripId:     payload['trip_id']   as string | null,
          });
        } else {
          console.warn('[consumer] Unknown payment method:', method);
        }

      } else if (type === 'refund.requested') {
        const originalMethod = payload['payment_method'] as string;
        if (originalMethod === 'wallet') {
          await handleWalletRefund({
            paymentRef:         String(payload['payment_ref']),
            originalPaymentRef: String(payload['original_payment_ref']),
            userId:             String(payload['user_id']),
            amount:             bigintFromMsg(payload['ticket_price']),
            currency:           'RWF',
            ticketId:           payload['ticket_id'] as string | null,
            reason:             payload['reason'] as string | undefined,
          });
        } else {
          await handleMomoRefund({
            paymentRef:         String(payload['payment_ref']),
            originalPaymentRef: String(payload['original_payment_ref']),
            phone:              String(payload['phone']),
            userId:             payload['user_id'] as string | null,
            amount:             bigintFromMsg(payload['ticket_price']),
            currency:           'RWF',
            ticketId:           payload['ticket_id'] as string | null,
            reason:             payload['reason'] as string | undefined,
          });
        }

      } else {
        // Unknown type on this queue — ack and discard
        console.warn('[consumer] trips-payment-svc: ignoring unknown type:', type);
      }

      try { tripsCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] trips-payment-svc failed:', (err as Error).message);
      try { tripsCh.nack(msg, false, false); } catch { /* channel closed */ }
    }
  });

  // ── users-payment-svc ────────────────────────────────────────────────────
  // Consumes user.passenger.created and org.activated from the users exchange.
  // users is broker-predefined (definitions.json) so we checkExchange.
  const usersCh: Channel = await conn.createChannel();
  await usersCh.prefetch(1);
  await usersCh.checkExchange('users');
  await usersCh.checkExchange('users.dlx');

  await usersCh.assertQueue('users-payment-svc', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': 'users.dlx' },
  });
  await usersCh.bindQueue('users-payment-svc', 'users', 'user.passenger.created');
  await usersCh.bindQueue('users-payment-svc', 'users', 'org.activated');
  await usersCh.bindQueue('users-payment-svc', 'users', 'wallet.topup.requested');

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
      const type    = payload['type'] as string;

      if (type === 'user.passenger.created') {
        await createWallet(String(payload['userId']), 'PASSENGER');
      } else if (type === 'org.activated') {
        await createWallet(String(payload['orgId']), 'ORGANISATION');
      } else if (type === 'wallet.topup.requested') {
        await handleWalletTopup({
          topupId:   String(payload['topup_id']),
          topupRef:  String(payload['payment_ref']),
          userId:    String(payload['user_id']),
          phone:     String(payload['phone']),
          amount:    BigInt(String(payload['amount'])),
        });
      } else {
        console.warn('[consumer] users-payment-svc: ignoring unknown type:', type);
      }

      try { usersCh.ack(msg); } catch { /* channel closed */ }
    } catch (err) {
      console.error('[consumer] users-payment-svc failed:', (err as Error).message);
      try { usersCh.nack(msg, false, false); } catch { /* channel closed */ }
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
