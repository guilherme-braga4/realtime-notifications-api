import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
} from '@nestjs/websockets';
import { Subscription } from 'rxjs';
import { WebSocket } from 'ws';
import { DatabaseService } from '../database/database.service.js';
import { EventsListener } from './events.listener.js';
import { Item, Topic, TopicEvent, TOPICS } from './topic-event.js';

const HEARTBEAT_MS = 30_000;
const NEWS_IN_SNAPSHOT = 50;

/** Mensagens enviadas ao cliente (JSON). Toda mensagem diz de que tópico é. */
export type ServerMessage =
  | { type: 'snapshot'; topic: Topic; lastEventId: string; items: Item[] }
  | ({ type: 'event' } & TopicEvent);

type SnapshotRow = { last_event_id: string } & Record<Topic, Item[]>;

interface ClientState {
  /** Eventos que chegaram antes de os snapshots serem enviados; `null` depois. */
  pending: TopicEvent[] | null;
  /** Último evento já refletido nos snapshots enviados a este cliente. */
  lastEventId: bigint;
  /** Respondeu ao último ping? */
  alive: boolean;
}

/**
 * Entrega os tópicos em tempo real por WebSocket em `/ws`.
 *
 * Ao conectar, o cliente recebe um `snapshot` por tópico, com os itens
 * atuais, e daí em diante um `event` por item criado ou alterado, em ordem.
 * O `item` de um evento tem o mesmo formato dos itens do snapshot: basta
 * trocá-lo (ou incluí-lo) pela `key`.
 *
 * Para não haver buraco nem repetição entre os snapshots e o fluxo: o cliente
 * é registrado antes da leitura, os eventos que chegam nesse intervalo ficam
 * em fila, e só passam (da fila ou depois dela) os eventos com id maior que o
 * último já refletido nos snapshots. O segundo caso acontece quando o evento
 * já estava gravado na hora da leitura, mas o listener ainda não o tinha
 * entregue.
 */
@WebSocketGateway({ path: '/ws' })
export class EventsGateway
  implements
    OnModuleInit,
    OnModuleDestroy,
    OnGatewayConnection<WebSocket>,
    OnGatewayDisconnect<WebSocket>
{
  private readonly logger = new Logger(EventsGateway.name);
  private readonly clients = new Map<WebSocket, ClientState>();
  private subscription?: Subscription;
  private heartbeat?: NodeJS.Timeout;

  constructor(
    private readonly listener: EventsListener,
    private readonly db: DatabaseService,
  ) {}

  onModuleInit() {
    this.subscription = this.listener.events$.subscribe((event) =>
      this.broadcast(event),
    );
    this.heartbeat = setInterval(() => this.checkClients(), HEARTBEAT_MS);
  }

  onModuleDestroy() {
    this.subscription?.unsubscribe();
    clearInterval(this.heartbeat);
    for (const client of this.clients.keys()) {
      client.close(1001, 'server shutting down');
    }
    this.clients.clear();
  }

  async handleConnection(client: WebSocket) {
    const state: ClientState = { pending: [], lastEventId: 0n, alive: true };
    this.clients.set(client, state);
    client.on('pong', () => (state.alive = true));
    client.on('error', (error) =>
      this.logger.warn(`Client error: ${error.message}`),
    );
    this.logger.log(`Client connected (${this.clients.size} online)`);

    try {
      const snapshot = await this.readSnapshot();
      if (!this.clients.has(client)) {
        return; // desconectou durante a leitura
      }
      for (const topic of TOPICS) {
        const message: ServerMessage = {
          type: 'snapshot',
          topic,
          lastEventId: snapshot.last_event_id,
          items: snapshot[topic],
        };
        this.send(client, JSON.stringify(message));
      }

      state.lastEventId = BigInt(snapshot.last_event_id);
      const pending = state.pending ?? [];
      state.pending = null;
      for (const event of pending) {
        if (BigInt(event.id) > state.lastEventId) {
          this.send(client, this.serialize(event));
        }
      }
    } catch (error) {
      this.logger.error(`Failed to send snapshot: ${(error as Error).message}`);
      client.close(1011, 'snapshot unavailable');
    }
  }

  handleDisconnect(client: WebSocket) {
    if (this.clients.delete(client)) {
      this.logger.log(`Client disconnected (${this.clients.size} online)`);
    }
  }

  /** Todos os tópicos e o último evento lidos em um único comando: a mesma fotografia. */
  private async readSnapshot(): Promise<SnapshotRow> {
    const { rows } = await this.db.pool.query<SnapshotRow>(
      `select
         (select coalesce(max(id), 0)::text from events) as last_event_id,
         (select coalesce(jsonb_agg(price_item(p) order by p.name, p.store), '[]')
            from products p) as prices,
         (select coalesce(jsonb_agg(metric_item(m) order by m.key), '[]')
            from metrics m) as metrics,
         (select coalesce(jsonb_agg(stock_item(s) order by s.symbol), '[]')
            from stocks s) as stocks,
         (select coalesce(jsonb_agg(news_item(n) order by n.published_at desc, n.id desc), '[]')
            from news n
           where n.id in (select id from news order by published_at desc, id desc limit $1)) as news`,
      [NEWS_IN_SNAPSHOT],
    );
    return rows[0];
  }

  private broadcast(event: TopicEvent) {
    const message = this.serialize(event); // serializa uma vez para todos
    const id = BigInt(event.id);
    for (const [client, state] of this.clients) {
      if (state.pending) {
        state.pending.push(event);
      } else if (id > state.lastEventId) {
        this.send(client, message);
      }
    }
  }

  private serialize(event: TopicEvent): string {
    return JSON.stringify({ type: 'event', ...event } satisfies ServerMessage);
  }

  private send(client: WebSocket, message: string) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }

  /** Derruba quem não respondeu ao ping anterior e envia um novo ping. */
  private checkClients() {
    for (const [client, state] of this.clients) {
      if (!state.alive) {
        client.terminate();
        continue;
      }
      state.alive = false;
      client.ping();
    }
  }
}
