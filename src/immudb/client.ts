import { Client } from '@codenotary/immudb-node';
import { config } from '../config/env.js';

let immuClient: Client | null = null;

export async function connectImmuDB(): Promise<Client> {
  const client = new Client({
    host:     config.immudb.host,
    port:     config.immudb.port,
    user:     config.immudb.user,
    password: config.immudb.password,
    database: config.immudb.database,
  });
  // The 2.0.0-alpha SDK connects lazily on first call — no explicit connect().
  // Verify it works by running a lightweight query.
  await client.sqlQuery({ sql: 'SELECT 1' });
  immuClient = client;
  console.info('[immudb] Connected to', config.immudb.host);
  return client;
}

export function getImmuDB(): Client {
  if (!immuClient) throw new Error('[immudb] Not connected — call connectImmuDB() first');
  return immuClient;
}

export async function disconnectImmuDB(): Promise<void> {
  if (immuClient) {
    await immuClient.close().catch(() => undefined);
    immuClient = null;
  }
}
