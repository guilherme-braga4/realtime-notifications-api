import { readFileSync } from 'node:fs';
import { Client } from 'pg';

/** Banco exclusivo dos testes e2e, separado do banco da API local. */
export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  'postgres://postgres:postgres@localhost:5432/realtime_notifications_test';

/**
 * Global setup do Vitest: cria o banco de testes se não existir, aplica o
 * schema e limpa os dados da execução anterior.
 */
export default async function setup() {
  const url = new URL(TEST_DATABASE_URL);
  const database = url.pathname.slice(1);
  if (!database.endsWith('_test')) {
    throw new Error(
      `Refusing to run e2e tests against "${database}": the database name must end with "_test"`,
    );
  }

  const adminUrl = new URL(TEST_DATABASE_URL);
  adminUrl.pathname = '/postgres';
  const admin = new Client({ connectionString: adminUrl.toString() });
  await admin.connect();
  try {
    const { rowCount } = await admin.query(
      'select 1 from pg_database where datname = $1',
      [database],
    );
    if (rowCount === 0) {
      await admin.query(`create database "${database}"`);
    }
  } finally {
    await admin.end();
  }

  const client = new Client({ connectionString: TEST_DATABASE_URL });
  await client.connect();
  try {
    await client.query(readFileSync('db/schema.sql', 'utf8'));
    await client.query(
      'truncate products, price_changes, consumer_cursors restart identity cascade',
    );
  } finally {
    await client.end();
  }
}
