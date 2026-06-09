import { withImmuDB } from './client.js';

export async function initImmuDBSchema(): Promise<void> {
  await withImmuDB(async (db) => {
    await db.sqlExec({ sql: `
      CREATE TABLE IF NOT EXISTS payment_events (
        id          VARCHAR(36)   NOT NULL,
        event_type  VARCHAR(50)   NOT NULL,
        payment_ref VARCHAR(36)   NOT NULL,
        owner_id    VARCHAR(36)   NOT NULL,
        owner_type  VARCHAR(20)   NOT NULL,
        amount      INTEGER       NOT NULL,
        currency    VARCHAR(3)    NOT NULL,
        method      VARCHAR(10),
        status      VARCHAR(20)   NOT NULL,
        ticket_id   VARCHAR(36),
        trip_id     VARCHAR(36),
        org_id      VARCHAR(36),
        gateway_ref VARCHAR(36),
        occurred_at VARCHAR(40)   NOT NULL,
        metadata    VARCHAR(1000),
        PRIMARY KEY (id)
      )
    ` });

    try {
      await db.sqlExec({ sql: 'CREATE INDEX ON payment_events(payment_ref)' });
    } catch (e) {
      if (!(e as Error).message.includes('already exists')) throw e;
    }

    try {
      await db.sqlExec({ sql: 'CREATE INDEX ON payment_events(owner_id)' });
    } catch (e) {
      if (!(e as Error).message.includes('already exists')) throw e;
    }
  });

  console.info('[immudb] Schema initialised');
}
