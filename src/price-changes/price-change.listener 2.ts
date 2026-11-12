import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Client } from 'pg';
import { Observable, Subject } from 'rxjs';
import { DatabaseService } from '../database/database.service.js';
import { PRICE_CHANGED_CHANNEL, PriceChange } from './price-change.js';

const DEFAULT_CONSUMER = 'price-change-listener';
const BATCH_SIZE = 500;
const INITIAL_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;

interface PriceChangeRow {
  id: string;
  product_id: number;
  name: string;
  store: string;
  old_price: string;
  new_price: string;
  changed_at: Date;
}

/**
 * Entrega em `changes$`, em ordem, os eventos da tabela `price_changes`.
 *
 * - A tabela é a fonte dos eventos. O NOTIFY do Postgres, recebido em uma
 *   conexão dedicada (LISTEN exige conexão persistente, fora do pool), é só o
 *   sinal para ler o que há de novo.
 * - O cursor é o id do último evento entregue. Fica em memória e é salvo em
 *   `consumer_cursors` após cada lote, para retomar do mesmo ponto ao reiniciar.
 * - Ao (re)conectar, lê tudo o que está depois do cursor, então eventos
 *   gravados durante uma queda não se perdem.
 * - A entrega é "pelo menos uma vez": se o processo cair entre emitir um lote
 *   e salvar o cursor, o lote é emitido de novo ao reiniciar. Quem consome
 *   deve usar `PriceChange.id` para ignorar repetidos.
 * - Na primeira execução de um consumidor, o cursor começa no último evento
 *   existente: o histórico anterior não é reenviado.
 */
@Injectable()
export class PriceChangeListener
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(PriceChangeListener.name);
  private readonly changes = new Subject<PriceChange>();
  private readonly consumer: string;
  private cursor = '0';
  private client?: Client;
  private reconnectTimer?: NodeJS.Timeout;
  private drainRetryTimer?: NodeJS.Timeout;
  private failedAttempts = 0;
  private draining = false;
  private drainRequested = false;
  private closing = false;

  readonly changes$: Observable<PriceChange> = this.changes.asObservable();

  constructor(
    private readonly config: ConfigService,
    private readonly db: DatabaseService,
  ) {
    this.consumer =
      this.config.get<string>('PRICE_CHANGES_CONSUMER') ?? DEFAULT_CONSUMER;
  }

  // Começa só depois de todos os módulos iniciarem: quem assina `changes$` no
  // próprio onModuleInit não perde os eventos lidos na partida.
  async onApplicationBootstrap() {
    this.cursor = await this.loadCursor();
    this.logger.log(
      `Consumer "${this.consumer}" resuming after event ${this.cursor}`,
    );
    await this.connect();
  }

  async onModuleDestroy() {
    this.closing = true;
    clearTimeout(this.reconnectTimer);
    clearTimeout(this.drainRetryTimer);
    this.changes.complete();

    const client = this.client;
    this.client = undefined;
    await client?.end().catch(() => undefined);
  }

  /** Lê o cursor salvo; cria-o no último evento existente se for a primeira vez. */
  private async loadCursor(): Promise<string> {
    const { rows } = await this.db.pool.query<{ last_id: string }>(
      `insert into consumer_cursors (consumer, last_id)
       values ($1, (select coalesce(max(id), 0) from price_changes))
       on conflict (consumer) do update set consumer = excluded.consumer
       returning last_id::text`,
      [this.consumer],
    );
    return rows[0].last_id;
  }

  /** Nunca rejeita: em caso de falha, agenda uma nova tentativa. */
  private async connect() {
    const client = new Client({
      connectionString: this.config.getOrThrow<string>('DATABASE_URL'),
    });
    this.client = client;

    client.on('notification', () => void this.drain());
    client.on('error', (error) => this.handleDisconnect(client, error.message));
    client.on('end', () => this.handleDisconnect(client, 'connection ended'));

    try {
      await client.connect();
      await client.query(`LISTEN ${PRICE_CHANGED_CHANNEL}`);
    } catch (error) {
      this.handleDisconnect(client, (error as Error).message);
      return;
    }

    this.failedAttempts = 0;
    this.logger.log(
      `Listening for price changes on channel "${PRICE_CHANGED_CHANNEL}"`,
    );
    // Só depois do LISTEN: assim nada gravado entre a leitura e o LISTEN fica
    // sem aviso. Cobre também o que foi gravado enquanto estávamos fora.
    await this.drain();
  }

  /**
   * Lê e emite tudo o que está depois do cursor. Nunca rejeita e nunca roda em
   * paralelo: um aviso que chega durante uma leitura só pede mais uma rodada.
   */
  private async drain() {
    if (this.draining) {
      this.drainRequested = true;
      return;
    }
    this.draining = true;
    clearTimeout(this.drainRetryTimer);

    try {
      do {
        this.drainRequested = false;
        while (!this.closing && (await this.deliverNextBatch()) === BATCH_SIZE);
      } while (this.drainRequested && !this.closing);
    } catch (error) {
      if (!this.closing) {
        this.logger.error(
          `Failed to read price changes (${(error as Error).message}); retrying in ${INITIAL_RETRY_MS}ms`,
        );
        this.drainRetryTimer = setTimeout(
          () => void this.drain(),
          INITIAL_RETRY_MS,
        );
      }
    } finally {
      this.draining = false;
    }
  }

  /** Entrega um lote e salva o cursor. Retorna quantos eventos entregou. */
  private async deliverNextBatch(): Promise<number> {
    const { rows } = await this.db.pool.query<PriceChangeRow>(
      `select id::text, product_id, name, store,
              old_price::text, new_price::text, changed_at
         from price_changes
        where id > $1
        order by id
        limit $2`,
      [this.cursor, BATCH_SIZE],
    );
    if (rows.length === 0) {
      return 0;
    }

    for (const row of rows) {
      this.logger.log(
        `Price changed: [${row.store}] ${row.name} ${row.old_price} -> ${row.new_price} (event ${row.id}, product ${row.product_id})`,
      );
      this.changes.next({
        id: row.id,
        productId: row.product_id,
        name: row.name,
        store: row.store,
        oldPrice: row.old_price,
        newPrice: row.new_price,
        changedAt: row.changed_at.toISOString(),
      });
    }

    this.cursor = rows[rows.length - 1].id;
    // greatest(): outra instância com o mesmo nome pode já ter salvo um
    // ponto mais adiante; o cursor salvo nunca anda para trás.
    await this.db.pool.query(
      `update consumer_cursors
          set last_id = greatest(last_id, $2), updated_at = now()
        where consumer = $1`,
      [this.consumer, this.cursor],
    );
    return rows.length;
  }

  /** Idempotente por conexão: `error` e `end` podem chegar em sequência. */
  private handleDisconnect(client: Client, reason: string) {
    if (this.closing || this.client !== client) {
      return;
    }
    this.client = undefined;
    client.end().catch(() => undefined);

    const delay = Math.min(
      INITIAL_RETRY_MS * 2 ** this.failedAttempts,
      MAX_RETRY_MS,
    );
    this.failedAttempts += 1;
    this.logger.error(
      `Price change listener disconnected (${reason}); retrying in ${delay}ms`,
    );
    this.reconnectTimer = setTimeout(() => void this.connect(), delay);
  }
}
