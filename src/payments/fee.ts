import { config } from '../config/env.js';

// Ceiling division — always rounds UP so we never under-collect.
// Rwanda has no decimal currency.
function ceilBasisPoints(amount: bigint, basisPoints: bigint): bigint {
  if (basisPoints === BigInt(0)) return BigInt(0);
  return (amount * basisPoints + BigInt(9999)) / BigInt(10000);
}

// Platform fee — our cut, deducted from the org's settlement.
// Applied at payment confirmation time.
export function computeFee(amount: bigint): bigint {
  return ceilBasisPoints(amount, BigInt(config.platformFeeBasisPoints));
}

// Gateway markup — added to the amount sent to FDI to cover their charge.
// Applied at payment request time (fdiPull) for MoMo topups and ticket payments.
export function computeGatewayMarkup(amount: bigint): bigint {
  return ceilBasisPoints(amount, BigInt(config.gatewayFeeBasisPoints));
}
