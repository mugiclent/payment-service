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
  feeAmount?: bigint | null;
  netAmount?: bigint | null;
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
  topupId: string;
  topupRef: string;
  userId: string;
  method: string;
  amount: bigint;
  newBalance: bigint;
  confirmedAt: string;
}): void {
  publish(EXCHANGE, 'topup.confirmed', data);
}

export function publishTopupFailed(data: {
  topupId: string;
  topupRef: string;
  userId: string;
  method?: string;
  amount: bigint;
  reason: string;
  failedAt: string;
}): void {
  publish(EXCHANGE, 'topup.failed', data);
}

export function publishPassengerTransaction(data: {
  userId: string;
  newBalance: bigint;
  movement: 'DEBIT' | 'CREDIT';
  method: string;
  amount: bigint;
  occurredAt: string;
  source: 'topup' | 'ticket_payment' | 'refund';
  reference: string;
  ticketId?: string | null;
}): void {
  publish(EXCHANGE, 'wallet.events', { type: 'passenger.transaction', ...data });
}

export function publishOrganisationTransaction(data: {
  orgId: string;
  newBalance: bigint;
  movement: 'DEBIT' | 'CREDIT';
  amount: bigint;
  occurredAt: string;
  source: 'ticket_payment' | 'refund';
  reference: string;
  ticketId?: string | null;
}): void {
  publish(EXCHANGE, 'wallet.events', { type: 'organisation.transaction', ...data });
}
