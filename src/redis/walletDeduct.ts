import { redis } from './client.js';

const WALLET_DEDUCT_SCRIPT = `
local balance = tonumber(redis.call('GET', KEYS[1]))
if balance == nil then
  return -2
end
local amount = tonumber(ARGV[1])
if balance < amount then
  return -1
end
local newBalance = balance - amount
redis.call('SET', KEYS[1], tostring(newBalance), 'KEEPTTL')
return newBalance
`;

export function walletKey(ownerId: string): string {
  return `wallet:balance:${ownerId}`;
}

/**
 * Atomically deducts amount from wallet in Redis.
 * Returns:
 *   >= 0   new balance after deduction
 *   -1     insufficient funds
 *   -2     cache miss — caller must reload from PostgreSQL and retry
 */
export async function atomicWalletDeduct(
  ownerId: string,
  amount: bigint,
): Promise<number> {
  const result = await redis.eval(
    WALLET_DEDUCT_SCRIPT,
    1,
    walletKey(ownerId),
    amount.toString(),
  );
  return result as number;
}
