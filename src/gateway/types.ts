export interface PaymentWebhookEvent {
  internalRef: string;
  gatewayRef?: string;
  status: 'SUCCESSFUL' | 'FAILED';
  amount: number;
  provider: string;
  rawPayload: object;
}
