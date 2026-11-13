import { AddressInfo } from 'node:net';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { WebSocket } from 'ws';
import { AppModule } from './../src/app.module.js';
import { setupApp } from './../src/app.setup.js';
import { DatabaseService } from './../src/database/database.service.js';
import { ServerMessage } from './../src/events/events.gateway.js';
import { TOPICS } from './../src/events/topic-event.js';

type Snapshot = Extract<ServerMessage, { type: 'snapshot' }>;
type EventMessage = Extract<ServerMessage, { type: 'event' }>;

// Roda no banco de testes (ver test/test-database.ts). Precisa do Postgres no
// ar: docker compose up -d --wait.
describe('EventsGateway (e2e)', () => {
  // Nome do consumidor e de todos os itens criados por este arquivo.
  const name = `gateway-e2e-${Date.now()}-${Math.random()}`;
  let app: INestApplication;
  let db: DatabaseService;
  let port: number;
  let productId: number;
  const sockets: WebSocket[] = [];

  /** Conecta em /ws e coleta os snapshots e os eventos dos itens de teste. */
  const connect = async () => {
    const socket = new WebSocket(`ws://localhost:${port}/ws`);
    sockets.push(socket);
    const snapshots: Snapshot[] = [];
    const events: EventMessage[] = [];

    socket.on('message', (data) => {
      const message = JSON.parse((data as Buffer).toString()) as ServerMessage;
      if (message.type === 'snapshot') {
        snapshots.push(message);
      } else if (
        [message.key, message.item.name, message.item.title].includes(name)
      ) {
        events.push(message);
      }
    });
    await vi.waitFor(() => expect(snapshots).toHaveLength(TOPICS.length));
    const snapshot = (topic: string) =>
      snapshots.find((message) => message.topic === topic)!;
    return { snapshots, snapshot, events };
  };

  const setPrice = (price: string) =>
    db.pool.query(
      'update products set price = $1, updated_at = now() where id = $2',
      [price, productId],
    );

  beforeAll(async () => {
    process.env.EVENTS_CONSUMER = name;
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleRef.createNestApplication();
    setupApp(app);
    await app.listen(0);
    port = (app.getHttpServer().address() as AddressInfo).port;

    db = app.get(DatabaseService);
    const { rows } = await db.pool.query<{ id: number }>(
      `insert into products (name, store, price) values ($1, 'e2e', 100)
       returning id`,
      [name],
    );
    productId = rows[0].id;
    await db.pool.query(
      `insert into metrics (key, label, unit, value) values ($1, 'Energia', 'TWh', 10)`,
      [name],
    );
    await db.pool.query(
      `insert into stocks (symbol, name, price, previous_close) values ($1, $1, 100, 100)`,
      [name],
    );
    await db.pool.query(
      `insert into news (title, url, source) values ($1, 'https://example.com/a', 'e2e')`,
      [name],
    );
  });

  afterEach(() => {
    for (const socket of sockets.splice(0)) {
      socket.close();
    }
  });

  afterAll(async () => {
    await app.close();
    delete process.env.EVENTS_CONSUMER;
  });

  it('sends one snapshot per topic with the current items on connect', async () => {
    const { snapshots, snapshot } = await connect();

    expect(snapshots.map((message) => message.topic)).toEqual([...TOPICS]);
    expect(snapshot('prices').items).toContainEqual(
      expect.objectContaining({
        id: productId,
        name,
        store: 'e2e',
        price: '100.00',
      }),
    );
    expect(snapshot('metrics').items).toContainEqual(
      expect.objectContaining({
        key: name,
        label: 'Energia',
        unit: 'TWh',
        value: '10',
      }),
    );
    expect(snapshot('stocks').items).toContainEqual(
      expect.objectContaining({
        symbol: name,
        price: '100.00',
        previousClose: '100.00',
      }),
    );
    expect(snapshot('news').items).toContainEqual(
      expect.objectContaining({ title: name, source: 'e2e' }),
    );
    // Todos saem da mesma fotografia do banco.
    expect(new Set(snapshots.map((message) => message.lastEventId)).size).toBe(
      1,
    );
  });

  it('pushes each event to every connected client, in order', async () => {
    const first = await connect();
    const second = await connect();

    await setPrice('90.00');
    await db.pool.query('update metrics set value = 11 where key = $1', [name]);
    await setPrice('80.00');

    for (const client of [first, second]) {
      await vi.waitFor(() => expect(client.events).toHaveLength(3));
      expect(client.events).toMatchObject([
        {
          type: 'event',
          topic: 'prices',
          key: String(productId),
          item: { id: productId, name, store: 'e2e', price: '90.00' },
          previous: { price: '100.00' },
        },
        {
          topic: 'metrics',
          key: name,
          item: { value: '11' },
          previous: { value: '10' },
        },
        {
          topic: 'prices',
          item: { price: '80.00' },
          previous: { price: '90.00' },
        },
      ]);
    }
  });

  it('sends an event item in the same format as the snapshot item', async () => {
    const { events } = await connect();
    await db.pool.query('update stocks set price = 87 where symbol = $1', [
      name,
    ]);
    await vi.waitFor(() => expect(events).toHaveLength(1));

    const { snapshot } = await connect();
    expect(snapshot('stocks').items).toContainEqual(events[0].item);
  });

  it('does not resend a change already reflected in the snapshot', async () => {
    await setPrice('70.00');
    const { snapshot, events } = await connect();

    expect(snapshot('prices').items).toContainEqual(
      expect.objectContaining({ id: productId, price: '70.00' }),
    );

    await setPrice('60.00');
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({
      item: { price: '60.00' },
      previous: { price: '70.00' },
    });
  });

  it('stops sending to a client that disconnected', async () => {
    const gone = await connect();
    const stays = await connect();
    sockets[0].close();
    await vi.waitFor(() =>
      expect(sockets[0].readyState).toBe(WebSocket.CLOSED),
    );

    await setPrice('50.00');

    await vi.waitFor(() => expect(stays.events).toHaveLength(1));
    expect(gone.events).toHaveLength(0);
  });
});
