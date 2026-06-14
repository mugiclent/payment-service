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

let isShuttingDown        = false;
let isReconnecting        = false;
let isReconnectingChannel = false;

const RETRY_DELAY_MS = 3_000;

function bigintFromMsg(val: unknown): bigint {
  return BigInt(String(val));
}

function parse(content: Buffer): unknown {
  return JSON.parse(content.toString('utf8'));
}

async function setupConsumerChannels(): Promise<void> {
  const conn = getConnection();

  // ── trips-payment-svc ────────────────────────────────────────────────────
  // trip-service publishes all ticket lifecycle events to the `trips` exchange
  // under the coarse `ticket.events` routing key, with the specific event in a
  // `type` field (payment.requested / refund.requested / ticket.confirmed). We
  // bind that coarse key and dispatch on `type` below.
  const tripsCh: Channel = await conn.createChannel();
  await tripsCh.prefetch(1);
  await tripsCh.checkExchange('trips');
  await tripsCh.checkExchange(TRIPS_DLX);

  await tripsCh.assertQueue('trips-payment-svc', {
    durable: true,
    arguments: { 'x-dead-letter-exchange': TRIPS_DLX },
  });
  await tripsCh.bindQueue('trips-payment-svc', 'trips', 'ticket.events');

  // Remove the legacy fine-grained bindings — trip-service never published to
  // these keys. Idempotent: unbinding an absent binding is a no-op.
  for (const legacy of ['payment.requested', 'refund.requested']) {
    await tripsCh.unbindQueue('trips-payment-svc', 'trips', legacy);
  }

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

      }
      // Other ticket.events (e.g. ticket.confirmed) are not relevant to
      // payments — ack and ignore.

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
  // user-service publishes domain events to the `users` topic exchange with
  // coarse per-domain routing keys (user.events / org.events / wallet.events)
  // and the specific event in a `type` field — same convention trip-service uses.
  await usersCh.bindQueue('users-payment-svc', 'users', 'user.events');
  await usersCh.bindQueue('users-payment-svc', 'users', 'org.events');
  await usersCh.bindQueue('users-payment-svc', 'users', 'wallet.events');

  // Remove the legacy fine-grained bindings from before the routing-key
  // convention fix — user-service never published to these keys. Idempotent:
  // unbinding an absent binding is a no-op, so this is safe on every reconnect.
  for (const legacy of ['user.passenger.created', 'org.activated', 'wallet.topup.requested']) {
    await usersCh.unbindQueue('users-payment-svc', 'users', legacy);
  }

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

      // A passenger account activating is the trigger to provision its wallet.
      // user-service emits `user.activated` for both passengers and staff, so
      // filter on user_type. (There is no dedicated passenger-created event.)
      if (type === 'user.activated' && payload['user_type'] === 'passenger') {
        await createWallet(String(payload['id']), 'PASSENGER');
      } else if (type === 'org.activated') {
        await createWallet(String(payload['id']), 'ORGANISATION');
      } else if (type === 'wallet.topup.requested') {
        await handleWalletTopup({
          topupId:   String(payload['topup_id']),
          topupRef:  String(payload['payment_ref']),
          userId:    String(payload['user_id']),
          phone:     String(payload['phone']),
          amount:    BigInt(String(payload['amount'])),
        });
      }
      // Other user/org/wallet events (staff.*, user.password_changed, …) are not
      // relevant to payments — ack and ignore.

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
