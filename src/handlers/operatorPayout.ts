import { prisma } from '../db/prisma.js';
import { v4 as uuidv4 } from 'uuid';

export interface OperatorPayoutInput {
  orgId:         string;
  operatorName:  string;
  bankAccount:   string;
  amount:        bigint;
  currency:      string;
  reference:     string;
}

// TODO: Implement bank transfer via BNR-approved payment rail
//       when that capability is available.
export async function handleOperatorPayout(input: OperatorPayoutInput): Promise<void> {
  const { orgId, operatorName, bankAccount, amount, currency, reference } = input;

  console.info('[payout] Operator payout queued:', reference);

  await prisma.outboxEntry.create({
    data: {
      id:         uuidv4(),
      eventType:  'OPERATOR_PAYOUT_PENDING',
      paymentRef: reference,
      payload: {
        id:          uuidv4(),
        event_type:  'OPERATOR_PAYOUT_PENDING',
        payment_ref: reference,
        owner_id:    orgId,
        owner_type:  'ORGANISATION',
        amount:      Number(amount),
        currency,
        method:      null,
        status:      'PENDING',
        ticket_id:   null,
        trip_id:     null,
        org_id:      orgId,
        gateway_ref: null,
        occurred_at: new Date().toISOString(),
        metadata:    JSON.stringify({ operatorName, bankAccount }),
      },
    },
  });
}
