import type { PaymentWebhookEvent } from '../types.js';

/**
 * Handles both FDI webhook callback format and status-poll format.
 *
 * Callback: { status, data: { state, gwRef, trxRef, channelRef? } }
 * Poll:     { status, data: { trxStatus, id (gwRef), trxRef, ... } }
 */
export function normaliseFdi(body: unknown): PaymentWebhookEvent {
  const b = body as Record<string, unknown>;
  const data = (b['data'] ?? {}) as Record<string, unknown>;

  const internalRef   = (data['trxRef'] ?? b['trxRef'] ?? '') as string;
  const gatewayRef    = (data['gwRef'] ?? data['id']) as string | undefined;
  const failureReason = (data['message'] ?? data['reason']) as string | undefined;

  // Webhook uses `state`, poll uses `trxStatus` — both lowercase
  const rawStatus = (data['state'] ?? data['trxStatus'] ?? '') as string;
  const status: 'SUCCESSFUL' | 'FAILED' =
    rawStatus.toLowerCase() === 'successful' ? 'SUCCESSFUL' : 'FAILED';

  const amount = Math.round(Number(data['amount'] ?? b['amount'] ?? 0));

  return {
    internalRef,
    gatewayRef,
    status,
    failureReason: status === 'FAILED' ? failureReason : undefined,
    amount,
    provider: 'fdi',
    rawPayload: body as object,
  };
}
