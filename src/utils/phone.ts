type MomoMethod = 'mtn' | 'airtel';

// Rwanda MoMo prefixes (strip leading 0 or +250/250 before matching)
const MTN_PREFIXES    = ['78', '79'];
const AIRTEL_PREFIXES = ['72', '73'];

export function inferMomoMethod(phone: string): MomoMethod {
  const digits = phone.replace(/^\+?250/, '').replace(/^0/, '');

  const prefix = digits.slice(0, 2);
  if (MTN_PREFIXES.includes(prefix))    return 'mtn';
  if (AIRTEL_PREFIXES.includes(prefix)) return 'airtel';

  throw new Error(`Cannot determine MoMo network for phone: ${phone}`);
}

export function momoChannelId(method: MomoMethod, mtnId: string, airtelId: string): string {
  return method === 'mtn' ? mtnId : airtelId;
}
