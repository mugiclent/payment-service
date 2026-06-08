import Fastify from 'fastify';
import { internalRoutes } from './routes/internal.js';
import { webhookRoutes } from './routes/webhooks.js';
import { healthRoutes } from './routes/health.js';

export function buildApp() {
  const app = Fastify({ logger: false });

  app.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      done(null, JSON.parse(body as string));
    } catch (err) {
      done(err as Error, undefined);
    }
  });

  void app.register(internalRoutes);
  void app.register(webhookRoutes);
  void app.register(healthRoutes);

  return app;
}
