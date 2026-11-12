import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DatabaseService.name);
  private readonly target: string;
  readonly pool: Pool;

  constructor(config: ConfigService) {
    const connectionString = config.getOrThrow<string>('DATABASE_URL');
    const { host, pathname } = new URL(connectionString);
    this.target = `${host}${pathname}`;

    this.pool = new Pool({ connectionString });
    this.pool.on('error', (error) =>
      this.logger.error(`Postgres connection error: ${error.message}`),
    );
  }

  async onModuleInit() {
    this.logger.log(`Connecting to Postgres at ${this.target}...`);
    try {
      const { rows } = await this.pool.query<{ version: string; user: string }>(
        `select current_setting('server_version') as version, current_user as "user"`,
      );
      this.logger.log(
        `Connected to Postgres ${rows[0].version} at ${this.target} as "${rows[0].user}"`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to connect to Postgres at ${this.target}: ${(error as Error).message}`,
      );
      throw error;
    }
  }

  async onModuleDestroy() {
    await this.pool.end();
    this.logger.log('Postgres connection closed');
  }
}
