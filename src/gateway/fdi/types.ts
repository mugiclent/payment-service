export interface FdiAuthResponse {
  status: string;
  data: {
    token: string;
    expires_at: string;
  };
}

export interface FdiProcessingData {
  trxRef: string;
  gwRef: string;
  state: string;
}

export interface FdiProcessingResponse {
  status: string;
  data: FdiProcessingData;
}

export interface FdiTransactionData {
  id: string;
  walletId: string;
  trxRef: string;
  trxType: string;
  channelRef?: string | null;
  channelId?: string | null;
  channelMsg?: string | null;
  channelTrxStatus?: string | null;
  channelStatusCode?: number | null;
  amount: number;
  trxFees: number;
  currency: string;
  trxStatus: 'successful' | 'failed' | 'pending';
  createdAt: string;
  callback?: string | null;
  msisdn: string;
}

export interface FdiTransactionResponse {
  status: string;
  data: FdiTransactionData;
}

export interface FdiRefundData {
  trxRef: string;
  gwRef: string;
  state: string;
}

export interface FdiRefundResponse {
  status: string;
  data: FdiRefundData;
}

export interface FdiChannelModel {
  id: string;
  categoryId: 'momo' | 'cash';
  label: string;
  state: 'up' | 'down' | 'unstable' | 'disabled';
  currency: string;
  minTrxAmount: number;
  maxTrxAmount: number;
}

export interface FdiChannelsResponse {
  status: string;
  data: {
    channels: FdiChannelModel[];
  };
}

export interface FdiMomoCallbackPayload {
  status: 'success' | 'fail';
  data: {
    state: string;
    gwRef: string;
    trxRef: string;
    channelRef?: string;
    message?: string;
  };
}
