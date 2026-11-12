import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
} from '@nestjs/websockets';
import { Subscription } from 'rxjs';
import { WebSocket } from 'ws';
import { DatabaseService } from '../database/database.service.js';
import { PriceChange } from './price-change.js';
import { PriceChangeListener } from './price-change.listener.js';

const HEARTBEAT_MS = 30_000;

interface ProductSnapshot {
  id: number;
  name: string;
  store: string;
  price: string;
  updatedAt: string;
}

/** Mensagens enviadas ao cliente (JSON). */
export type ServerMessage =
  | { type: 'snapshot'; lastEventId: string; products: ProductSnapshot[] }
  | { type: 'price_change'; change: PriceChange };

interface ClientState {
  /** Eventos que chegaram antes de o snapshot ser enviado; `null` depois. */
  pending: PriceChange[] | null;
  /** Respondeu ao último ping? */
  alive: boolean;
}

/**
 * Entrega preços em tempo real por WebSocket em `/ws`.
 *
 * Ao conectar, o cliente recebe um `snapshot` com o preço atual de todos os
 * produtos e, daí em diante, um `price_change` por mudança, em ordem.
 *
 * Para não haver buraco nem repetição entre o snapshot e o fluxo: o cliente é
 * registrado antes da leitura do snapshot, os eventos que chegam nesse
 * intervalo ficam em fila, e ao liberar a fila só passam os eventos com id
 * maior que o último já refletido no snapshot.
 */
@WebSocketGateway({ path: '/ws' })
export class PriceChangesGateway
  implements
    OnModuleInit,
    OnModuleDestroy,
    OnGatewayConnection<WebSocket>,
    OnGatewayDisconnect<WebSocket>
{
  private readonly logger = new Logger(PriceChangesGateway.name);
  private readonly clients = new Map<WebSocket, ClientState>();
  private subscription?: Subscription;
  private heartbeat?: NodeJS.Timeout;

  constructor(
    private readonly listener: PriceChangeListener,
    private readonly db: DatabaseService,
  ) {}

  onModuleInit() {
    this.subscription = this.listener.changes$.subscribe((change) =>
      this.broadcast(change),
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
    const state: ClientState = { pending: [], alive: true };
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
      this.send(client, JSON.stringify(snapshot));

      const lastEventId = BigInt(snapshot.lastEventId);
      const pending = state.pending ?? [];
      state.pending = null;
      for (const change of pending) {
        if (BigInt(change.id) > lastEventId) {
          this.send(client, this.serialize(change));
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

  /** Produtos e último evento lidos em um único comando: a mesma fotografia. */
  private async readSnapshot(): Promise<ServerMessage & { type: 'snapshot' }> {
    const { rows } = await this.db.pool.query<{
      last_event_id: string;
      products: ProductSnapshot[] | null;
    }>(
      `select
         (select coalesce(max(id), 0)::text from price_changes) as last_event_id,
         (select json_agg(json_build_object(
                   'id', id, 'name', name, 'store', store,
                   'price', price::text, 'updatedAt', updated_at)
                 order by name, store)
            from products) as products`,
    );
    return {
      type: 'snapshot',
      lastEventId: rows[0].last_event_id,
      products: rows[0].products ?? [],
    };
  }

  private broadcast(change: PriceChange) {
    const message = this.serialize(change); // serializa uma vez para todos
    for (const [client, state] of this.clients) {
      if (state.pending) {
        state.pending.push(change);
      } else {
        this.send(client, message);
      }
    }
  }

  private serialize(change: PriceChange): string {
    return JSON.stringify({
      type: 'price_change',
      change,
    } satisfies ServerMessage);
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
