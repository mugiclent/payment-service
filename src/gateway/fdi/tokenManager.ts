import { redis } from '../../redis/client.js';
import { config } from '../../config/env.js';
import type { FdiAuthResponse } from './types.js';

const TOKEN_KEY = 'fdi:token';
// Renew this many seconds before the Redis key would evict
const PROACTIVE_BUFFER_S = 90;
const RETRY_DELAY_MS = 30_000;

export class FdiTokenManager {
  private refreshTimer: NodeJS.Timeout | null = null;
  private stopped = false;

  async getToken(): Promise<string> {
    const cached = await redis.get(TOKEN_KEY);
    if (cached) return cached;

    return this.fetchAndCache();
  }

  private async fetchAndCache(): Promise<string> {
    const res = await fetch(`${config.fdi.baseUrl}/rw/v3/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ appId: config.fdi.appId, secret: config.fdi.secret }),
    });

    if (!res.ok) {
      throw new Error(`[fdi] Auth failed: HTTP ${res.status}`);
    }

    const body = (await res.json()) as FdiAuthResponse;
    const { token, expires_at } = body.data;

    // FDI returns expires_at without a timezone suffix ("2026-06-09T17:52:49.680281").
    // V8 parses bare ISO strings as local time, not UTC — appending Z forces UTC.
    const expiresAtUtc = expires_at.endsWith('Z') ? expires_at : `${expires_at}Z`;

    // Cache expires 60 s before FDI token does — getToken() cache-miss triggers
    // a new fetch, but the background refresher fires even earlier (PROACTIVE_BUFFER_S)
    // so cache misses in getToken() only happen on cold start or error recovery.
    const ttl = Math.floor((new Date(expiresAtUtc).getTime() - Date.now()) / 1000) - 60;
    if (ttl > 0) {
      await redis.set(TOKEN_KEY, token, 'EX', ttl);
      this.scheduleRefresh(ttl);
    }

    return token;
  }

  /**
   * Schedules the next proactive refresh at (remainingTtl - PROACTIVE_BUFFER_S).
   * If the remaining TTL is already within the buffer, refreshes immediately.
   */
  private scheduleRefresh(remainingTtlS: number): void {
    if (this.stopped) return;

    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }

    const delayMs = Math.max(0, (remainingTtlS - PROACTIVE_BUFFER_S) * 1000);

    this.refreshTimer = setTimeout(() => {
      if (this.stopped) return;
      void this.proactiveRefresh();
    }, delayMs);

    // Prevent the timer from blocking process exit
    this.refreshTimer.unref();
  }

  private async proactiveRefresh(): Promise<void> {
    try {
      await this.fetchAndCache();
      console.info('[fdi] Token proactively refreshed');
    } catch (err) {
      console.warn('[fdi] Proactive token refresh failed:', (err as Error).message);
      // Retry after a short delay rather than letting the token silently expire
      if (!this.stopped) {
        this.refreshTimer = setTimeout(() => {
          void this.proactiveRefresh();
        }, RETRY_DELAY_MS);
        this.refreshTimer.unref();
      }
    }
  }

  async invalidate(): Promise<void> {
    await redis.del(TOKEN_KEY);
  }

  /**
   * Fetches a token on startup, then starts the background refresh loop.
   * After this call getToken() will always return from cache.
   */
  async warmup(): Promise<void> {
    try {
      // Check if there is already a cached token (e.g. another instance refreshed it)
      const ttl = await redis.ttl(TOKEN_KEY);
      if (ttl > PROACTIVE_BUFFER_S) {
        // Token is still fresh — schedule refresh based on remaining TTL
        this.scheduleRefresh(ttl);
        console.info(`[fdi] Token already cached, refresh in ${ttl - PROACTIVE_BUFFER_S}s`);
        return;
      }

      await this.fetchAndCache();
      console.info('[fdi] Token cached, background refresh scheduled');
    } catch (err) {
      console.warn('[fdi] Token warmup failed:', (err as Error).message);
      // Schedule a retry so the refresher eventually recovers without a restart
      this.scheduleRefresh(RETRY_DELAY_MS / 1000);
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
  }
}

export const tokenManager = new FdiTokenManager();
