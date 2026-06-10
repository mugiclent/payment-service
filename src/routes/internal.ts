import type { FastifyInstance } from 'fastify';
import { prisma } from '../db/prisma.js';
import { getBalance } from '../wallet/balance.js';
import { handleOperatorPayout } from '../handlers/operatorPayout.js';
import type { Prisma } from '@prisma/client';

export async function internalRoutes(app: FastifyInstance): Promise<void> {
  // GET /internal/wallet/balance/:ownerId
  app.get<{ Params: { ownerId: string } }>(
    '/internal/wallet/balance/:ownerId',
    async (req, reply) => {
      const { ownerId } = req.params;
      try {
        const balance = await getBalance(ownerId);
        return reply.send({
          ownerId,
          balance:  balance.toString(),
          currency: 'RWF',
        });
      } catch (err) {
        return reply.status(404).send({ error: (err as Error).message });
      }
    },
  );

  /**
   * GET /internal/transactions
   *
   * Query parameters:
   *   paymentRef  — exact match
   *   ownerId     — matches userId OR phone
   *   orgId       — exact match
   *   method      — wallet | mtn | airtel | cash
   *   type        — TICKET_PAYMENT | WALLET_TOPUP | REFUND
   *   status      — PENDING | CONFIRMED | FAILED | REFUNDED
   *   from        — ISO 8601 date, inclusive (createdAt >=)
   *   to          — ISO 8601 date, inclusive (createdAt <=)
   *   page        — default 1
   *   limit       — default 20, max 100
   */
  app.get<{
    Querystring: {
      paymentRef?: string;
      ownerId?:    string;
      orgId?:      string;
      method?:     string;
      type?:       string;
      status?:     string;
      from?:       string;
      to?:         string;
      page?:       string;
      limit?:      string;
    };
  }>(
    '/internal/transactions',
    async (req, reply) => {
      const q     = req.query;
      const page  = Math.max(1, parseInt(q.page  ?? '1',  10));
      const limit = Math.min(100, parseInt(q.limit ?? '20', 10));

      const where: Prisma.TransactionWhereInput = {};

      if (q.paymentRef) where.paymentRef = q.paymentRef;
      if (q.orgId)      where.orgId      = q.orgId;
      if (q.method)     where.method     = q.method as Prisma.EnumPaymentMethodFilter;
      if (q.type)       where.type       = q.type   as Prisma.EnumTransactionTypeFilter;
      if (q.status)     where.status     = q.status as Prisma.EnumPaymentStatusFilter;

      if (q.ownerId) {
        where.OR = [{ userId: q.ownerId }, { phone: q.ownerId }];
      }

      if (q.from || q.to) {
        where.createdAt = {};
        if (q.from) where.createdAt.gte = new Date(q.from);
        if (q.to)   where.createdAt.lte = new Date(q.to);
      }

      const [rows, total] = await Promise.all([
        prisma.transaction.findMany({
          where,
          orderBy: { createdAt: 'desc' },
          skip:    (page - 1) * limit,
          take:    limit,
        }),
        prisma.transaction.count({ where }),
      ]);

      return reply.send({
        data: rows.map((t) => ({
          ...t,
          amount:    t.amount.toString(),
          feeAmount: t.feeAmount?.toString() ?? null,
          netAmount: t.feeAmount != null ? (t.amount - t.feeAmount).toString() : null,
        })),
        meta: { page, limit, total, pages: Math.ceil(total / limit) },
      });
    },
  );

  // POST /internal/billing/disburse
  app.post<{
    Body: {
      orgId:         string;
      operatorName:  string;
      bankAccount:   string;
      amount:        string;
      currency:      string;
      reference:     string;
    };
  }>(
    '/internal/billing/disburse',
    async (req, reply) => {
      const { orgId, operatorName, bankAccount, amount, currency, reference } = req.body;
      if (!orgId || !operatorName || !bankAccount || !amount || !currency || !reference) {
        return reply.status(400).send({ error: 'Missing required fields' });
      }

      await handleOperatorPayout({
        orgId,
        operatorName,
        bankAccount,
        amount:   BigInt(amount),
        currency,
        reference,
      });

      return reply.status(202).send({ reference });
    },
  );
}
