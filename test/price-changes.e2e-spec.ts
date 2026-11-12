import { readFileSync } from 'node:fs';
import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { DatabaseModule } from './../src/database/database.module.js';
import { DatabaseService } from './../src/database/database.service.js';
import { PriceChange } from './../src/price-changes/price-change.js';
import { PriceChangeListener } from './../src/price-changes/price-change.listener.js';
import { PriceChangesModule } from './../src/price-changes/price-changes.module.js';

// Precisa do Postgres no ar (docker compose up -d --wait).
describe('PriceChangeListener (e2e)', () => {
  let moduleRef: TestingModule;
  let db: DatabaseService;
  let productId: number;
  let received: PriceChange[];

  const setPrice = (price: string) =>
    db.pool.query(
      'update products set price = $1, updated_at = now() where id = $2',
      [price, productId],
    );

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        DatabaseModule,
        PriceChangesModule,
      ],
    }).compile();
    await moduleRef.init();

    db = moduleRef.get(DatabaseService);
    await db.pool.query(readFileSync('db/schema.sql', 'utf8'));

    const { rows } = await db.pool.query<{ id: number }>(
      `insert into products (name, store, price) values ($1, 'e2e', 100)
       returning id`,
      [`e2e-${Date.now()}-${Math.random()}`],
    );
    productId = rows[0].id;

    received = [];
    moduleRef.get(PriceChangeListener).changes$.subscribe((change) => {
      if (change.productId === productId) {
        received.push(change);
      }
    });
  });

  afterAll(async () => {
    await db.pool.query('delete from products where id = $1', [productId]);
    await moduleRef.close();
  });

  beforeEach(() => {
    received.length = 0;
  });

  it('emits a change when a price is updated', async () => {
    await setPrice('89.90');

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({
      productId,
      store: 'e2e',
      oldPrice: '100.00',
      newPrice: '89.90',
    });
  });

  it('does not emit when the price stays the same', async () => {
    await db.pool.query('update products set store = $1 where id = $2', [
      'e2e',
      productId,
    ]);
    await setPrice('89.90');
    await setPrice('79.90');

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({ oldPrice: '89.90', newPrice: '79.90' });
  });

  it('reconnects and resumes after the connection is dropped', async () => {
    await db.pool.query(
      `select pg_terminate_backend(pid) from pg_stat_activity
        where query = 'LISTEN price_changed' and pid <> pg_backend_pid()`,
    );

    let price = 50;
    await vi.waitFor(
      async () => {
        price += 1;
        await setPrice(price.toFixed(2));
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(received.length).toBeGreaterThan(0);
      },
      { timeout: 10_000, interval: 300 },
    );
  }, 15_000);
});
