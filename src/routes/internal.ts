import type { FastifyInstance } from 'fastify';
import { prisma } from '../db/prisma.js';
import { getBalance } from '../wallet/balance.js';
import { handleWalletTopup } from '../handlers/walletTopup.js';
import { handleOperatorPayout } from '../handlers/operatorPayout.js';

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
          balance: balance.toString(),
          currency: 'RWF',
        });
      } catch (err) {
        return reply.status(404).send({ error: (err as Error).message });
      }
    },
  );

  // GET /internal/wallet/transactions/:ownerId
  app.get<{
    Params: { ownerId: string };
    Querystring: { page?: string; limit?: string };
  }>(
    '/internal/wallet/transactions/:ownerId',
    async (req, reply) => {
      const { ownerId } = req.params;
      const page  = Math.max(1, parseInt(req.query.page  ?? '1',  10));
      const limit = Math.min(100, parseInt(req.query.limit ?? '20', 10));

      const [entries, total] = await Promise.all([
        prisma.walletLedger.findMany({
          where:   { ownerId },
          orderBy: { createdAt: 'desc' },
          skip:    (page - 1) * limit,
          take:    limit,
        }),
        prisma.walletLedger.count({ where: { ownerId } }),
      ]);

      return reply.send({
        data: entries.map((e) => ({
          ...e,
          amount:        e.amount.toString(),
          balanceBefore: e.balanceBefore.toString(),
          balanceAfter:  e.balanceAfter.toString(),
        })),
        meta: { page, limit, total },
      });
    },
  );

  // POST /internal/topup
  app.post<{
    Body: { topupRef: string; userId: string; phone: string; amount: string };
  }>(
    '/internal/topup',
    async (req, reply) => {
      const { topupRef, userId, phone, amount } = req.body;
      if (!topupRef || !userId || !phone || !amount) {
        return reply.status(400).send({ error: 'Missing required fields' });
      }

      try {
        await handleWalletTopup({
          topupRef,
          userId,
          phone,
          amount: BigInt(amount),
        });
        return reply.status(202).send({ topupRef });
      } catch (err) {
        return reply.status(500).send({ error: (err as Error).message });
      }
    },
  );

  // GET /internal/transactions/:paymentRef
  app.get<{ Params: { paymentRef: string } }>(
    '/internal/transactions/:paymentRef',
    async (req, reply) => {
      const trx = await prisma.transaction.findUnique({
        where: { paymentRef: req.params.paymentRef },
      });
      if (!trx) return reply.status(404).send({ error: 'Transaction not found' });
      return reply.send({
        ...trx,
        amount: trx.amount.toString(),
      });
    },
  );

  // POST /internal/billing/disburse
  app.post<{
    Body: {
      orgId: string;
      operatorName: string;
      bankAccount: string;
      amount: string;
      currency: string;
      reference: string;
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
        amount: BigInt(amount),
        currency,
        reference,
      });

      return reply.status(202).send({ reference });
    },
  );
}
