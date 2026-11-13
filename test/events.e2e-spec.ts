import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { DatabaseModule } from './../src/database/database.module.js';
import { DatabaseService } from './../src/database/database.service.js';
import { EventsListener } from './../src/events/events.listener.js';
import { EventsModule } from './../src/events/events.module.js';
import { TopicEvent } from './../src/events/topic-event.js';

// Roda no banco de testes (ver test/test-database.ts). Precisa do Postgres no
// ar: docker compose up -d --wait.
describe('Events (e2e)', () => {
  // Nome do consumidor e de todos os itens criados por este arquivo.
  const name = `e2e-${Date.now()}-${Math.random()}`;
  let moduleRef: TestingModule;
  let db: DatabaseService;
  let productId: number;
  let received: TopicEvent[];

  /** Sobe a "API": banco + listener, coletando os eventos dos itens de teste. */
  const start = async () => {
    moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        DatabaseModule,
        EventsModule,
      ],
    }).compile();
    // Assina antes de iniciar, para receber também o que é lido na partida.
    moduleRef.get(EventsListener).events$.subscribe((event) => {
      if ([event.key, event.item.name, event.item.title].includes(name)) {
        received.push(event);
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

  const newPrices = () => received.map((event) => event.item.price);

  /** O que o trigger gravou em events para o produto de teste. */
  const recordedEvents = async () => {
    const { rows } = await db.pool.query<{
      id: string;
      old_price: string;
      new_price: string;
    }>(
      `select id::text, previous->>'price' as old_price, item->>'price' as new_price
         from events
        where topic = 'prices' and key = $1
        order by events.id`,
      [String(productId)],
    );
    return rows;
  };

  const savedCursor = async () => {
    const { rows } = await db.pool.query<{ last_id: string }>(
      'select last_id::text from consumer_cursors where consumer = $1',
      [name],
    );
    return rows[0]?.last_id;
  };

  beforeAll(async () => {
    process.env.EVENTS_CONSUMER = name;
    received = [];
    await start();

    const { rows } = await db.pool.query<{ id: number }>(
      `insert into products (name, store, price) values ($1, 'e2e', 100)
       returning id`,
      [name],
    );
    productId = rows[0].id;

    // A criação do produto também é um evento, sem estado anterior.
    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({
      topic: 'prices',
      key: String(productId),
      item: { id: productId, name, store: 'e2e', price: '100.00' },
      previous: null,
    });
  });

  afterAll(async () => {
    await moduleRef.close();
    delete process.env.EVENTS_CONSUMER;
  });

  beforeEach(async () => {
    received.length = 0;
    await db.pool.query(
      `delete from events where topic = 'prices' and key = $1`,
      [String(productId)],
    );
  });

  it('records the change in events and emits that same event', async () => {
    await setPrice('89.90');

    const recorded = await recordedEvents();
    expect(recorded).toHaveLength(1);
    expect(recorded[0]).toMatchObject({
      old_price: '100.00',
      new_price: '89.90',
    });

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(received[0]).toMatchObject({
      id: recorded[0].id,
      topic: 'prices',
      key: String(productId),
      item: { id: productId, name, store: 'e2e', price: '89.90' },
      previous: { price: '100.00' },
    });
    expect(Number.isNaN(Date.parse(received[0].at))).toBe(false);
  });

  it('records nothing when only the update date changes', async () => {
    await db.pool.query(
      'update products set updated_at = now() where id = $1',
      [productId],
    );
    await setPrice('89.90');
    await setPrice('79.90');

    await vi.waitFor(() => expect(received).toHaveLength(1));
    expect(await recordedEvents()).toHaveLength(1);
    expect(received[0]).toMatchObject({
      item: { price: '79.90' },
      previous: { price: '89.90' },
    });
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
    expect(received[0]).toMatchObject({
      item: { price: '78.00' },
      previous: { price: '79.90' },
    });
  });

  it('emits events in the order they were recorded and saves the cursor', async () => {
    for (const price of ['70.00', '60.00', '50.00', '40.00']) {
      await setPrice(price);
    }

    await vi.waitFor(() => expect(received).toHaveLength(4));
    const recorded = await recordedEvents();
    expect(received.map((event) => event.id)).toEqual(
      recorded.map((row) => row.id),
    );
    expect(newPrices()).toEqual(['70.00', '60.00', '50.00', '40.00']);

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
    expect(newPrices()).toEqual(prices);
    const ids = received.map((event) => BigInt(event.id));
    expect(ids).toEqual([...ids].sort((a, b) => (a < b ? -1 : 1)));
  });

  it('delivers changes made while the listener connection was down', async () => {
    await db.pool.query(
      `select pg_terminate_backend(pid) from pg_stat_activity
        where query = 'LISTEN events'
          and datname = current_database()
          and pid <> pg_backend_pid()`,
    );
    // O listener só reconecta após 1s: estas mudanças acontecem com ele fora.
    await setPrice('39.00');
    await setPrice('38.00');

    await vi.waitFor(() => expect(received).toHaveLength(2), {
      timeout: 10_000,
    });
    expect(newPrices()).toEqual(['39.00', '38.00']);
  }, 15_000);

  it('resumes from the saved cursor after a restart, without repeating events', async () => {
    await setPrice('37.00');
    await vi.waitFor(() => expect(received).toHaveLength(1));
    const [before] = await recordedEvents();
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
    expect(newPrices()).toEqual(['36.00', '35.00']);
  });

  it('emits events for metrics, stocks and news, each in its own topic', async () => {
    await db.pool.query(
      `insert into metrics (key, label, unit, value) values ($1, 'Energia', 'TWh', 10)`,
      [name],
    );
    await db.pool.query('update metrics set value = 10.5 where key = $1', [
      name,
    ]);
    await db.pool.query(
      `insert into stocks (symbol, name, price, previous_close) values ($1, $1, 100, 100)`,
      [name],
    );
    await db.pool.query('update stocks set price = 87 where symbol = $1', [
      name,
    ]);
    await db.pool.query(
      `insert into news (title, url, source) values ($1, 'https://example.com/a', 'e2e')`,
      [name],
    );

    await vi.waitFor(() => expect(received).toHaveLength(5));
    expect(received).toMatchObject([
      { topic: 'metrics', key: name, item: { value: '10' }, previous: null },
      {
        topic: 'metrics',
        key: name,
        item: { label: 'Energia', unit: 'TWh', value: '10.5' },
        previous: { value: '10' },
      },
      { topic: 'stocks', key: name, previous: null },
      {
        topic: 'stocks',
        key: name,
        item: { symbol: name, price: '87.00', previousClose: '100.00' },
        previous: { price: '100.00' },
      },
      {
        topic: 'news',
        item: { title: name, url: 'https://example.com/a', source: 'e2e' },
        previous: null,
      },
    ]);
  });
});
