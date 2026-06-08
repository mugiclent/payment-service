import { publish } from './client.js';

const EXCHANGE = 'payment';

export function publishPaymentConfirmed(data: {
  paymentRef: string;
  method: string;
  amount: bigint;
  currency: string;
  userId?: string | null;
  phone?: string | null;
  ticketId?: string | null;
  tripId?: string | null;
  orgId?: string | null;
  confirmedAt: string;
  gatewayRef?: string | null;
}): void {
  publish(EXCHANGE, 'payment.confirmed', data);
}

export function publishPaymentFailed(data: {
  paymentRef: string;
  method: string;
  amount: bigint;
  userId?: string | null;
  phone?: string | null;
  ticketId?: string | null;
  reason: string;
  failedAt: string;
  retryable: boolean;
}): void {
  publish(EXCHANGE, 'payment.failed', data);
}

export function publishTopupConfirmed(data: {
  topupRef: string;
  userId: string;
  amount: bigint;
  newBalance: bigint;
  confirmedAt: string;
}): void {
  publish(EXCHANGE, 'topup.confirmed', data);
}

export function publishTopupFailed(data: {
  topupRef: string;
  userId: string;
  amount: bigint;
  reason: string;
  failedAt: string;
}): void {
  publish(EXCHANGE, 'topup.failed', data);
}

/**
 * CQRS projection — User Service consumes this to update its display balance.
 * Only published for PASSENGER wallet operations.
 */
export function publishWalletTransactionCompleted(data: {
  userId: string;
  newBalance: bigint;
  type: string;
  amount: bigint;
  occurredAt: string;
}): void {
  publish(EXCHANGE, 'wallet.transaction.completed', data);
}
