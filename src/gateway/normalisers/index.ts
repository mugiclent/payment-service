import type { PaymentWebhookEvent } from '../types.js';
import { normaliseFdi } from './fdi.js';
import { normaliseMtn } from './mtn.js';
import { normaliseAirtel } from './airtel.js';

export const normalisers: Record<string, (body: unknown) => PaymentWebhookEvent> = {
  fdi:    normaliseFdi,
  mtn:    normaliseMtn,
  airtel: normaliseAirtel,
};
