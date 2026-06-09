import { config } from '../../config/env.js';
import { tokenManager } from './tokenManager.js';
import type {
  FdiChannelsResponse,
  FdiProcessingResponse,
  FdiRefundResponse,
  FdiTransactionResponse,
} from './types.js';

async function fdiRequest<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const token = await tokenManager.getToken();
  const url = `${config.fdi.baseUrl}${path}`;
  const opts: RequestInit = {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  };

  console.info('[fdi]', method, path);

  let res = await fetch(url, opts);

  if (res.status === 401) {
    await tokenManager.invalidate();
    const freshToken = await tokenManager.getToken();
    res = await fetch(url, {
      ...opts,
      headers: { ...opts.headers as Record<string, string>, Authorization: `Bearer ${freshToken}` },
    });
    if (res.status === 401) {
      throw new Error('[fdi] 401 after token refresh');
    }
  }

  return res.json() as Promise<T>;
}

export async function fdiPull(params: {
  trxRef: string;
  channelId: string;
  msisdn: string;
  amount: bigint;
}): Promise<FdiProcessingResponse> {
  const res = await fdiRequest<FdiProcessingResponse>('POST', '/momo/pull', {
    trxRef:    params.trxRef,
    channelId: params.channelId,
    walletId:  config.fdi.walletId,
    msisdn:    params.msisdn,
    amount:    Number(params.amount),
    callback:  config.fdi.callbackUrl,
  });
  if (res.status !== 'success' || !res.data?.gwRef) {
    const msg = (res.data as unknown as Record<string, string>)?.message ?? 'FDI pull failed';
    throw new Error(msg);
  }
  return res;
}

export async function fdiPush(params: {
  trxRef: string;
  channelId: string;
  msisdn: string;
  amount: bigint;
}): Promise<FdiProcessingResponse> {
  const res = await fdiRequest<FdiProcessingResponse>('POST', '/momo/push', {
    trxRef:    params.trxRef,
    channelId: params.channelId,
    walletId:  config.fdi.walletId,
    msisdn:    params.msisdn,
    amount:    Number(params.amount),
    callback:  config.fdi.callbackUrl,
  });
  if (res.status !== 'success' || !res.data?.gwRef) {
    const msg = (res.data as unknown as Record<string, string>)?.message ?? 'FDI push failed';
    throw new Error(msg);
  }
  return res;
}

export async function fdiRefund(params: {
  trxID: string;
  msisdn: string;
  amount: bigint;
}): Promise<FdiRefundResponse> {
  const res = await fdiRequest<FdiRefundResponse>('POST', '/momo/refund', {
    trxID:    params.trxID,
    msisdn:   params.msisdn,
    amount:   Number(params.amount),
    callback: config.fdi.callbackUrl,
  });
  if (res.status !== 'success' || !res.data?.gwRef) {
    const msg = (res.data as unknown as Record<string, string>)?.message ?? 'FDI refund failed';
    throw new Error(msg);
  }
  return res;
}

export async function fdiGetTransactionInfo(trxRef: string): Promise<FdiTransactionResponse> {
  return fdiRequest<FdiTransactionResponse>('GET', `/momo/${trxRef}/info`);
}

export async function fdiGetChannels(): Promise<FdiChannelsResponse> {
  return fdiRequest<FdiChannelsResponse>('GET', '/channels');
}
