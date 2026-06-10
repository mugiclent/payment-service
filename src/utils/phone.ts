type MomoMethod = 'mtn' | 'airtel';

// Rwanda MoMo prefixes (matched after stripping country code and leading zero)
const MTN_PREFIXES    = ['78', '79'];
const AIRTEL_PREFIXES = ['72', '73'];

// Accepted forms:
//   +2507XXXXXXXX  — E.164 with plus
//    2507XXXXXXXX  — E.164 without plus
//      07XXXXXXXX  — local with leading zero
//       7XXXXXXXX  — local without leading zero (9 digits)
// Any character other than digits and a leading + is rejected.
const VALID_PHONE_RE = /^\+?[\d]{7,15}$/;

export function validatePhone(phone: string): void {
  if (!VALID_PHONE_RE.test(phone)) {
    throw new Error(`Invalid phone number format: ${phone}`);
  }
}

export function inferMomoMethod(phone: string): MomoMethod {
  validatePhone(phone);

  const digits = phone.replace(/^\+?250/, '').replace(/^0/, '');

  const prefix = digits.slice(0, 2);
  if (MTN_PREFIXES.includes(prefix))    return 'mtn';
  if (AIRTEL_PREFIXES.includes(prefix)) return 'airtel';

  throw new Error(`Cannot determine MoMo network for phone: ${phone}`);
}

export function momoChannelId(method: MomoMethod, mtnId: string, airtelId: string): string {
  return method === 'mtn' ? mtnId : airtelId;
}
