const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isValidUuid(val: string): boolean {
  return UUID_RE.test(val);
}

export function assertUuid(name: string, val: string): void {
  if (!isValidUuid(val)) {
    throw new Error(`Invalid UUID for ${name}: ${val}`);
  }
}

export function assertUuidIfPresent(name: string, val: string | null | undefined): void {
  if (val != null && val !== '') assertUuid(name, val);
}
