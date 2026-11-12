import { Subject } from 'rxjs';
import { WebSocket } from 'ws';
import { DatabaseService } from '../database/database.service.js';
import { PriceChange } from './price-change.js';
import { PriceChangeListener } from './price-change.listener.js';
import { PriceChangesGateway } from './price-changes.gateway.js';

const change = (id: string): PriceChange => ({
  id,
  productId: 1,
  name: 'Air Fryer 5L',
  store: 'Shopee',
  oldPrice: '420.00',
  newPrice: '398.70',
  changedAt: '2026-11-27T03:00:00.000Z',
});

/** O mínimo de um WebSocket que o gateway usa. */
const fakeClient = () => {
  const sent: { type: string; change?: PriceChange; lastEventId?: string }[] =
    [];
  const client = {
    readyState: WebSocket.OPEN as number,
    on: vi.fn(),
    send: vi.fn((message: string) => sent.push(JSON.parse(message))),
    close: vi.fn(),
  };
  return { client: client as unknown as WebSocket, sent, close: client.close };
};

describe('PriceChangesGateway', () => {
  let changes: Subject<PriceChange>;
  let gateway: PriceChangesGateway;
  let resolveSnapshot: (lastEventId: string) => void;
  let rejectSnapshot: (error: Error) => void;

  beforeEach(() => {
    vi.useFakeTimers();
    changes = new Subject();
    const query = vi.fn(
      () =>
        new Promise((resolve, reject) => {
          resolveSnapshot = (lastEventId) =>
            resolve({ rows: [{ last_event_id: lastEventId, products: [] }] });
          rejectSnapshot = reject;
        }),
    );
    gateway = new PriceChangesGateway(
      { changes$: changes.asObservable() } as PriceChangeListener,
      { pool: { query } } as unknown as DatabaseService,
    );
    gateway.onModuleInit();
  });

  afterEach(() => {
    gateway.onModuleDestroy();
    vi.useRealTimers();
  });

  it('holds changes until the snapshot is sent, then skips the ones it already reflects', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);

    changes.next(change('5')); // já refletido no snapshot
    changes.next(change('6'));
    changes.next(change('7'));
    expect(sent).toHaveLength(0);

    resolveSnapshot('5');
    await connecting;

    expect(sent.map((message) => message.type)).toEqual([
      'snapshot',
      'price_change',
      'price_change',
    ]);
    expect(sent.slice(1).map((message) => message.change?.id)).toEqual([
      '6',
      '7',
    ]);
  });

  it('compares event ids as numbers, not text', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);

    changes.next(change('10')); // '10' < '9' como texto
    resolveSnapshot('9');
    await connecting;

    expect(sent.map((message) => message.change?.id)).toEqual([
      undefined,
      '10',
    ]);
  });

  it('sends changes directly once the snapshot is out', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    resolveSnapshot('0');
    await connecting;

    changes.next(change('1'));

    expect(sent.map((message) => message.type)).toEqual([
      'snapshot',
      'price_change',
    ]);
  });

  it('sends nothing to a client that left while the snapshot was loading', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    gateway.handleDisconnect(client);

    changes.next(change('1'));
    resolveSnapshot('0');
    await connecting;

    expect(sent).toHaveLength(0);
  });

  it('closes the client when the snapshot cannot be read', async () => {
    const { client, sent, close } = fakeClient();
    const connecting = gateway.handleConnection(client);

    rejectSnapshot(new Error('database down'));
    await connecting;

    expect(sent).toHaveLength(0);
    expect(close).toHaveBeenCalledWith(1011, 'snapshot unavailable');
  });
});
