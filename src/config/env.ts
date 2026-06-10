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
    'NODE_ENV',
    'DB_USER',
    'DB_NAME',
    'DB_PASSWORD',
    'RABBITMQ_USER',
    'RABBITMQ_PASSWORD',
    'REDIS_PASSWORD',
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
    'PLATFORM_FEE_BASIS_POINTS',
    'GATEWAY_FEE_BASIS_POINTS',
  ];
  for (const name of required) require(name);
}

const dbUser     = require('DB_USER');
const dbName     = require('DB_NAME');
const dbPassword = require('DB_PASSWORD');

export const config = {
  port: parseInt(optional('PORT', '8099'), 10),
  platformFeeBasisPoints: parseInt(require('PLATFORM_FEE_BASIS_POINTS'), 10),
  gatewayFeeBasisPoints:  parseInt(require('GATEWAY_FEE_BASIS_POINTS'),  10),

  db: {
    url: `postgresql://${dbUser}:${dbPassword}@pgbouncer:6432/${dbName}?pgbouncer=true&connect_timeout=5&pool_timeout=5`,
  },

  redis: {
    url: `redis://:${require('REDIS_PASSWORD')}@redis:6379`,
  },

  rabbitmq: {
    url: `amqp://${require('RABBITMQ_USER')}:${require('RABBITMQ_PASSWORD')}@rabbitmq:5672`,
  },

  immudb: {
    host:     require('IMMUDB_HOST'),
    port:     parseInt(require('IMMUDB_PORT'), 10),
    user:     require('IMMUDB_USER'),
    password: require('IMMUDB_PASSWORD'),
    database: require('IMMUDB_DATABASE'),
  },

  fdi: {
    baseUrl:        require('FDI_BASE_URL'),
    appId:          require('FDI_APP_ID'),
    secret:         require('FDI_SECRET'),
    walletId:       require('FDI_WALLET_ID'),
    mtnChannelId:   optional('FDI_MTN_CHANNEL_ID', ''),
    airtelChannelId: optional('FDI_AIRTEL_CHANNEL_ID', ''),
    callbackUrl:    `${require('PAYMENT_CALLBACK_BASE_URL')}/webhooks/payment/callback?provider=fdi`,
  },
};
