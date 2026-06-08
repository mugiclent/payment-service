import type { FastifyInstance } from 'fastify';
import { normalisers } from '../gateway/normalisers/index.js';
import { webhookQueue } from '../queues/webhookQueue.js';

export async function webhookRoutes(app: FastifyInstance): Promise<void> {
  // POST /webhooks/payment/callback?provider=<name>
  app.post<{ Querystring: { provider?: string } }>(
    '/webhooks/payment/callback',
    async (req, reply) => {
      // Return 200 immediately — provider must not retry on timeout
      reply.status(200).send({ received: true });

      const provider  = req.query.provider ?? '';
      const normalise = normalisers[provider];

      if (!normalise) {
        console.warn('[webhook] Unknown provider:', provider);
        return;
      }

      try {
        const event = normalise(req.body);
        await webhookQueue.add('payment-event', event);
      } catch (err) {
        console.error('[webhook] Failed to queue event:', (err as Error).message);
      }
    },
  );
}
