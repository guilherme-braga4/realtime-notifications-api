import { Subject } from 'rxjs';
import { WebSocket } from 'ws';
import { DatabaseService } from '../database/database.service.js';
import { EventsGateway } from './events.gateway.js';
import { EventsListener } from './events.listener.js';
import { TopicEvent, TOPICS } from './topic-event.js';

const event = (id: string): TopicEvent => ({
  id,
  topic: 'stocks',
  key: 'NVDA',
  item: { symbol: 'NVDA', price: '98.70' },
  previous: { symbol: 'NVDA', price: '113.40' },
  at: '2026-11-12T14:30:00.000Z',
});

/** O mínimo de um WebSocket que o gateway usa. */
const fakeClient = () => {
  const sent: { type: string; topic: string; id?: string }[] = [];
  const client = {
    readyState: WebSocket.OPEN as number,
    on: vi.fn(),
    send: vi.fn((message: string) => sent.push(JSON.parse(message))),
    close: vi.fn(),
  };
  return { client: client as unknown as WebSocket, sent, close: client.close };
};

describe('EventsGateway', () => {
  let events: Subject<TopicEvent>;
  let gateway: EventsGateway;
  let resolveSnapshot: (lastEventId: string) => void;
  let rejectSnapshot: (error: Error) => void;

  beforeEach(() => {
    vi.useFakeTimers();
    events = new Subject();
    const query = vi.fn(
      () =>
        new Promise((resolve, reject) => {
          resolveSnapshot = (lastEventId) =>
            resolve({
              rows: [
                {
                  last_event_id: lastEventId,
                  prices: [],
                  metrics: [],
                  stocks: [{ symbol: 'NVDA', price: '113.40' }],
                  news: [],
                },
              ],
            });
          rejectSnapshot = reject;
        }),
    );
    gateway = new EventsGateway(
      { events$: events.asObservable() } as EventsListener,
      { pool: { query } } as unknown as DatabaseService,
    );
    gateway.onModuleInit();
  });

  afterEach(() => {
    gateway.onModuleDestroy();
    vi.useRealTimers();
  });

  it('sends one snapshot per topic, each with its items and the last event id', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    resolveSnapshot('5');
    await connecting;

    expect(sent.map((message) => message.topic)).toEqual([...TOPICS]);
    expect(sent).toContainEqual({
      type: 'snapshot',
      topic: 'stocks',
      lastEventId: '5',
      items: [{ symbol: 'NVDA', price: '113.40' }],
    });
  });

  it('holds events until the snapshots are sent, then skips the ones they already reflect', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);

    events.next(event('5')); // já refletido nos snapshots
    events.next(event('6'));
    events.next(event('7'));
    expect(sent).toHaveLength(0);

    resolveSnapshot('5');
    await connecting;

    expect(sent.slice(0, TOPICS.length).map((message) => message.type)).toEqual(
      TOPICS.map(() => 'snapshot'),
    );
    expect(sent.slice(TOPICS.length)).toEqual([
      { type: 'event', ...event('6') },
      { type: 'event', ...event('7') },
    ]);
  });

  it('compares event ids as numbers, not text', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);

    events.next(event('10')); // '10' < '9' como texto
    resolveSnapshot('9');
    await connecting;

    expect(sent.slice(TOPICS.length).map((message) => message.id)).toEqual([
      '10',
    ]);
  });

  it('sends events directly once the snapshots are out', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    resolveSnapshot('0');
    await connecting;

    events.next(event('1'));

    expect(sent.at(-1)).toEqual({ type: 'event', ...event('1') });
  });

  it('skips an event the snapshots already reflect, even if it arrives after them', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    resolveSnapshot('5');
    await connecting;

    events.next(event('5')); // gravado antes da leitura, entregue depois
    events.next(event('6'));

    expect(sent.slice(TOPICS.length).map((message) => message.id)).toEqual([
      '6',
    ]);
  });

  it('sends nothing to a client that left while the snapshots were loading', async () => {
    const { client, sent } = fakeClient();
    const connecting = gateway.handleConnection(client);
    gateway.handleDisconnect(client);

    events.next(event('1'));
    resolveSnapshot('0');
    await connecting;

    expect(sent).toHaveLength(0);
  });

  it('closes the client when the snapshots cannot be read', async () => {
    const { client, sent, close } = fakeClient();
    const connecting = gateway.handleConnection(client);

    rejectSnapshot(new Error('database down'));
    await connecting;

    expect(sent).toHaveLength(0);
    expect(close).toHaveBeenCalledWith(1011, 'snapshot unavailable');
  });
});
