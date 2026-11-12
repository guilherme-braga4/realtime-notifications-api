import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Client } from 'pg';
import { Observable, Subject } from 'rxjs';
import {
  PRICE_CHANGED_CHANNEL,
  PriceChange,
  parsePriceChange,
} from './price-change.js';

const INITIAL_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;

/**
 * Escuta o canal `price_changed` do Postgres em uma conexão dedicada
 * (LISTEN exige uma conexão persistente, fora do pool) e expõe as mudanças
 * em `changes$`.
 *
 * LISTEN/NOTIFY não guarda mensagens: o que for publicado enquanto a conexão
 * estiver fora do ar não é entregue. A conexão é refeita com backoff
 * exponencial, e a perda é registrada no log.
 */
@Injectable()
export class PriceChangeListener implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PriceChangeListener.name);
  private readonly changes = new Subject<PriceChange>();
  private client?: Client;
  private retryTimer?: NodeJS.Timeout;
  private failedAttempts = 0;
  private closing = false;

  readonly changes$: Observable<PriceChange> = this.changes.asObservable();

  constructor(private readonly config: ConfigService) {}

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    this.closing = true;
    clearTimeout(this.retryTimer);
    this.changes.complete();

    const client = this.client;
    this.client = undefined;
    await client?.end().catch(() => undefined);
  }

  /** Nunca rejeita: em caso de falha, agenda uma nova tentativa. */
  private async connect() {
    const client = new Client({
      connectionString: this.config.getOrThrow<string>('DATABASE_URL'),
    });
    this.client = client;

    client.on('notification', (message) => this.handle(message.payload));
    client.on('error', (error) => this.handleDisconnect(client, error.message));
    client.on('end', () => this.handleDisconnect(client, 'connection ended'));

    try {
      await client.connect();
      await client.query(`LISTEN ${PRICE_CHANGED_CHANNEL}`);
    } catch (error) {
      this.handleDisconnect(client, (error as Error).message);
      return;
    }

    if (this.failedAttempts > 0) {
      this.logger.warn(
        'Reconnected to Postgres; price changes published while disconnected were not delivered',
      );
    }
    this.failedAttempts = 0;
    this.logger.log(
      `Listening for price changes on channel "${PRICE_CHANGED_CHANNEL}"`,
    );
  }

  private handle(payload: string | undefined) {
    let change: PriceChange;
    try {
      change = parsePriceChange(payload);
    } catch (error) {
      this.logger.warn(
        `Ignoring invalid notification (${(error as Error).message}): ${payload}`,
      );
      return;
    }

    this.logger.log(
      `Price changed: [${change.store}] ${change.name} ${change.oldPrice} -> ${change.newPrice} (product ${change.productId})`,
    );
    this.changes.next(change);
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
    this.retryTimer = setTimeout(() => void this.connect(), delay);
  }
}
