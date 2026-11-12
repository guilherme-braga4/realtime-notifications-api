import { AddressInfo } from 'node:net';
import { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import { WebSocket } from 'ws';
import { AppModule } from './../src/app.module.js';
import { setupApp } from './../src/app.setup.js';
import { DatabaseService } from './../src/database/database.service.js';
import { ServerMessage } from './../src/price-changes/price-changes.gateway.js';

// Roda no banco de testes (ver test/test-database.ts). Precisa do Postgres no
// ar: docker compose up -d --wait.
describe('PriceChangesGateway (e2e)', () => {
  const name = `gateway-e2e-${Date.now()}-${Math.random()}`;
  let app: NestExpressApplication;
  let db: DatabaseService;
  let port: number;
  let productId: number;
  const sockets: WebSocket[] = [];

  /** Conecta em /ws e coleta as mensagens que dizem respeito ao produto de teste. */
  const connect = async () => {
    const socket = new WebSocket(`ws://localhost:${port}/ws`);
    sockets.push(socket);
    const snapshots: Extract<ServerMessage, { type: 'snapshot' }>[] = [];
    const changes: Extract<
      ServerMessage,
      { type: 'price_change' }
    >['change'][] = [];

    socket.on('message', (data) => {
      const message = JSON.parse((data as Buffer).toString()) as ServerMessage;
      if (message.type === 'snapshot') {
        snapshots.push(message);
      } else if (message.change.productId === productId) {
        changes.push(message.change);
      }
    });
    await vi.waitFor(() => expect(snapshots).toHaveLength(1));
    return { snapshot: snapshots[0], changes };
  };

  const setPrice = (price: string) =>
    db.pool.query(
      'update products set price = $1, updated_at = now() where id = $2',
      [price, productId],
    );

  beforeAll(async () => {
    process.env.PRICE_CHANGES_CONSUMER = name;
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleRef.createNestApplication<NestExpressApplication>();
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
  });

  afterEach(() => {
    for (const socket of sockets.splice(0)) {
      socket.close();
    }
  });

  afterAll(async () => {
    await app.close();
    delete process.env.PRICE_CHANGES_CONSUMER;
  });

  it('sends a snapshot with current prices on connect', async () => {
    const { snapshot } = await connect();

    expect(snapshot.products).toContainEqual(
      expect.objectContaining({
        id: productId,
        name,
        store: 'e2e',
        price: '100.00',
      }),
    );
    expect(BigInt(snapshot.lastEventId)).toBeGreaterThanOrEqual(0n);
  });

  it('pushes each price change to every connected client, in order', async () => {
    const first = await connect();
    const second = await connect();

    await setPrice('90.00');
    await setPrice('80.00');

    for (const client of [first, second]) {
      await vi.waitFor(() => expect(client.changes).toHaveLength(2));
      expect(client.changes.map((change) => change.newPrice)).toEqual([
        '90.00',
        '80.00',
      ]);
      expect(client.changes[0]).toMatchObject({
        productId,
        name,
        store: 'e2e',
        oldPrice: '100.00',
      });
    }
  });

  it('does not resend a change already reflected in the snapshot', async () => {
    await setPrice('70.00');
    const { snapshot, changes } = await connect();

    expect(snapshot.products).toContainEqual(
      expect.objectContaining({ id: productId, price: '70.00' }),
    );

    await setPrice('60.00');
    await vi.waitFor(() => expect(changes).toHaveLength(1));
    expect(changes[0]).toMatchObject({ oldPrice: '70.00', newPrice: '60.00' });
  });

  it('stops sending to a client that disconnected', async () => {
    const gone = await connect();
    const stays = await connect();
    sockets[0].close();
    await vi.waitFor(() =>
      expect(sockets[0].readyState).toBe(WebSocket.CLOSED),
    );

    await setPrice('50.00');

    await vi.waitFor(() => expect(stays.changes).toHaveLength(1));
    expect(gone.changes).toHaveLength(0);
  });

  it('serves the demo page', async () => {
    const response = await fetch(`http://localhost:${port}/`);

    expect(response.status).toBe(200);
    expect(await response.text()).toContain('Preços em tempo real');
  });
});
