export interface PaymentWebhookEvent {
  paymentRef: string;
  gatewayRef?: string;
  status: 'SUCCESSFUL' | 'FAILED';
  failureReason?: string;
  amount: number;
  provider: string;
  rawPayload: object;
}
