import type { PaymentWebhookEvent } from '../types.js';

export function normaliseAirtel(body: unknown): PaymentWebhookEvent {
  const b = body as Record<string, unknown>;
  const trx = (b['transaction'] ?? {}) as Record<string, unknown>;
  return {
    paymentRef: trx['id'] as string,
    status:      (trx['status'] as string) === 'TS' ? 'SUCCESSFUL' : 'FAILED',
    amount:      Math.round(Number(trx['amount'] ?? 0)),
    provider:    'airtel',
    rawPayload:  body as object,
  };
}
