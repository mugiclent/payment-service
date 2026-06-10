import { config } from '../config/env.js';

/**
 * Ceiling division: always rounds UP to Katisha's advantage.
 * Rwanda has no decimal currency, so we collect at least the full fee.
 *
 * Example at 1% (100 basis points):
 *   1000 RWF → fee = 10,  net = 990
 *   1001 RWF → fee = 11,  net = 990  (not 10.01 — rounds up)
 *    101 RWF → fee =  2,  net =  99  (not 1.01 — rounds up)
 */
export function computeFee(amount: bigint): bigint {
  const basisPoints = BigInt(config.platformFeeBasisPoints);
  if (basisPoints === BigInt(0)) return BigInt(0);
  return (amount * basisPoints + BigInt(9999)) / BigInt(10000);
}
