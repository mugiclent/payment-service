import 'dotenv/config';

function require(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required environment variable: ${name}`);
  return val;
}

function optional(name: string, fallback: string): string {
  return process.env[name] ?? fallback;
}

export function validateEnv(): void {
  const required = [
    'DATABASE_URL',
    'REDIS_URL',
    'RABBITMQ_URL',
    'IMMUDB_HOST',
    'IMMUDB_PORT',
    'IMMUDB_USER',
    'IMMUDB_PASSWORD',
    'IMMUDB_DATABASE',
    'FDI_BASE_URL',
    'FDI_APP_ID',
    'FDI_SECRET',
    'FDI_WALLET_ID',
    'PAYMENT_CALLBACK_BASE_URL',
  ];
  for (const name of required) require(name);
}

export const config = {
  port: parseInt(optional('PAYMENT_SERVICE_PORT', '8092'), 10),

  db: {
    url: require('DATABASE_URL'),
  },

  redis: {
    url: require('REDIS_URL'),
  },

  rabbitmq: {
    url: require('RABBITMQ_URL'),
  },

  immudb: {
    host:     require('IMMUDB_HOST'),
    port:     parseInt(require('IMMUDB_PORT'), 10),
    user:     require('IMMUDB_USER'),
    password: require('IMMUDB_PASSWORD'),
    database: require('IMMUDB_DATABASE'),
  },

  fdi: {
    baseUrl:       require('FDI_BASE_URL'),
    appId:         require('FDI_APP_ID'),
    secret:        require('FDI_SECRET'),
    walletId:      require('FDI_WALLET_ID'),
    mtnChannelId:  optional('FDI_MTN_CHANNEL_ID', ''),
    airtelChannelId: optional('FDI_AIRTEL_CHANNEL_ID', ''),
    callbackUrl:   `${require('PAYMENT_CALLBACK_BASE_URL')}/webhooks/payment/callback?provider=fdi`,
  },
};
