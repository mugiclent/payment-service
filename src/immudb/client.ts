import { ImmudbClient } from '@codenotary/immudb-node';
import { config } from '../config/env.js';

let immudbClient: ImmudbClient | null = null;

export async function connectImmuDB(): Promise<ImmudbClient> {
  const client = new ImmudbClient({
    host:     config.immudb.host,
    port:     config.immudb.port,
    user:     config.immudb.user,
    password: config.immudb.password,
    database: config.immudb.database,
  });
  await client.connect();
  immudbClient = client;
  console.info('[immudb] Connected to', config.immudb.host);
  return client;
}

export function getImmuDB(): ImmudbClient {
  if (!immudbClient) throw new Error('[immudb] Not connected — call connectImmuDB() first');
  return immudbClient;
}

export async function disconnectImmuDB(): Promise<void> {
  if (immudbClient) {
    await immudbClient.disconnect().catch(() => undefined);
    immudbClient = null;
  }
}
