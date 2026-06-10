import type { PaymentWebhookEvent } from '../types.js';

export function normaliseMtn(body: unknown): PaymentWebhookEvent {
  const b = body as Record<string, unknown>;
  return {
    paymentRef: b['externalId'] as string,
    status:      (b['status'] as string) === 'SUCCESSFUL' ? 'SUCCESSFUL' : 'FAILED',
    amount:      Math.round(Number(b['amount'] ?? 0)),
    provider:    'mtn',
    rawPayload:  body as object,
  };
}
