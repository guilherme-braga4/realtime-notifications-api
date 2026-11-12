import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { DatabaseModule } from './../src/database/database.module.js';
import { DatabaseService } from './../src/database/database.service.js';
import { PriceChange } from './../src/price-changes/price-change.js';
import { PriceChangeListener } from './../src/price-changes/price-change.listener.js';
import { PriceChangesModule } from './../src/price-changes/price-changes.module.js';

// Roda no banco de testes (ver test/test-database.ts). Precisa do Postgres no
// ar: docker compose up -d --wait.
describe('Price changes (e2e)', () => {
  const consumer = `e2e-${Date.now()}-${Math.random()}`;
  let moduleRef: TestingModule;
  let db: DatabaseService;
  let productId: number;
  let received: PriceChange[];

  /** Sobe a "API": banco + listener, coletando os eventos do produto de teste. */
  const start = async () => {
    moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        DatabaseModule,
        PriceChangesModule,
      ],
    }).compile();
    // Assina antes de iniciar, para receber também o que é lido na partida.
    moduleRef.get(PriceChangeListener).changes$.subscribe((change) => {
      if (change.productId === productId) {
        received.push(change);
      }
    });
    await moduleRef.init();
    db = moduleRef.get(DatabaseService);
  };

  const setPrice = (price: string) =>
    db.pool.query(
      'update products set price = $1, updated_at = now() where id = $2',
      [price, productId],
    );

  /** O que o trigger gravou em price_changes para o produto de teste. */
  const recordedChanges = async () => {
    const { rows } = await db.pool.query<{
      id: string;
      old_price: string;
      new_price: string;
    }>(
      `select id::text, old_price::text, new_price::text
         from price_changes
        where product_id = $1
        order by price_changes.id`,
      [productId],
    );
    return rows;
  };

  const savedCursor = async () => {
    const { rows } = await db.pool.query<{ last_id: string }>(
      'select last_id::text from consumer_cursors where consumer = $1',
      [consumer],
    );
    return rows[0]?.last_id;
  };

  beforeAll(async () => {
    process.env.PRICE_CHANGES_CONSUMER = consumer;
    received = [];
    await start();

    const { rows } = await db.pool.query<{ id: number }>(
      `insert into products (name, store, price) values ($1, 'e2e', 100)
       returning id`,
      [consumer],
    );
    productId = rows[0].id;
  });

  afterAll(async () => {
    await moduleRef.close();
    delete process.env.PRICE_CHANGES_CONSUMER;
  });

  beforeEach(async () => {
    received.length = 0;
    await db.pool.query('delete from price_changes where product_id = $1', [
      productId,
    ]);
  });

  it('records the change in price_changes and emits that same event', async () => {
    await setPrice('89.90');

    const recorded = await recordedChanges();
    expect(recorded).toHaveLength(1);
    expect(recorded[0]).toMatchObject({
      old_price: '100.00',
      new_price: '89.90',
    });

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({
      id: recorded[0].id,
      productId,
      name: consumer,
      store: 'e2e',
      oldPrice: '100.00',
      newPrice: '89.90',
    });
    expect(Number.isNaN(Date.parse(received[0].changedAt))).toBe(false);
  });

  it('records nothing when the price stays the same', async () => {
    await db.pool.query('update products set store = $1 where id = $2', [
      'e2e',
      productId,
    ]);
    await setPrice('89.90');
    await setPrice('79.90');

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(await recordedChanges()).toHaveLength(1);
    expect(received[0]).toMatchObject({ oldPrice: '89.90', newPrice: '79.90' });
  });

  it('does not record a change that was rolled back', async () => {
    const client = await db.pool.connect();
    try {
      await client.query('begin');
      await client.query('update products set price = 1 where id = $1', [
        productId,
      ]);
      await client.query('rollback');
    } finally {
      client.release();
    }
    await setPrice('78.00');

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({ oldPrice: '79.90', newPrice: '78.00' });
  });

  it('emits events in the order they were recorded and saves the cursor', async () => {
    for (const price of ['70.00', '60.00', '50.00', '40.00']) {
      await setPrice(price);
    }

    await vi.waitFor(() => expect(received).toHaveLength(4));
    const recorded = await recordedChanges();
    expect(received.map((change) => change.id)).toEqual(
      recorded.map((row) => row.id),
    );
    expect(received.map((change) => change.newPrice)).toEqual([
      '70.00',
      '60.00',
      '50.00',
      '40.00',
    ]);

    // Outros testes gravam no mesmo banco em paralelo, então o cursor pode já
    // estar além do último evento deste produto, mas nunca antes dele.
    const lastId = BigInt(recorded[recorded.length - 1].id);
    await vi.waitFor(async () =>
      expect(BigInt((await savedCursor()) ?? 0)).toBeGreaterThanOrEqual(lastId),
    );
  });

  it('keeps numeric order when ids gain a digit (9 -> 10, 99 -> 100)', async () => {
    const prices = Array.from({ length: 120 }, (_, i) => (200 + i).toFixed(2));
    for (const price of prices) {
      await setPrice(price);
    }

    await vi.waitFor(() => expect(received).toHaveLength(prices.length));
    expect(received.map((change) => change.newPrice)).toEqual(prices);
    const ids = received.map((change) => BigInt(change.id));
    expect(ids).toEqual([...ids].sort((a, b) => (a < b ? -1 : 1)));
  });

  it('delivers changes made while the listener connection was down', async () => {
    await db.pool.query(
      `select pg_terminate_backend(pid) from pg_stat_activity
        where query = 'LISTEN price_changed'
          and datname = current_database()
          and pid <> pg_backend_pid()`,
    );
    // O listener só reconecta após 1s: estas mudanças acontecem com ele fora.
    await setPrice('39.00');
    await setPrice('38.00');

    await vi.waitFor(() => expect(received).toHaveLength(2), {
      timeout: 10_000,
    });
    expect(received.map((change) => change.newPrice)).toEqual([
      '39.00',
      '38.00',
    ]);
  }, 15_000);

  it('resumes from the saved cursor after a restart, without repeating events', async () => {
    await setPrice('37.00');
    await vi.waitFor(() => expect(received).toHaveLength(1));
    const [before] = await recordedChanges();
    await vi.waitFor(async () =>
      expect(BigInt((await savedCursor()) ?? 0)).toBeGreaterThanOrEqual(
        BigInt(before.id),
      ),
    );

    // Com a API "fora": o trigger grava, ninguém escuta.
    await moduleRef.close();
    received.length = 0;
    const offline = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true }), DatabaseModule],
    }).compile();
    await offline.init();
    db = offline.get(DatabaseService);
    await setPrice('36.00');
    await setPrice('35.00');
    await offline.close();

    await start();

    await vi.waitFor(() => expect(received).toHaveLength(2));
    expect(received.map((change) => change.newPrice)).toEqual([
      '36.00',
      '35.00',
    ]);
  });
});
