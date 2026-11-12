// Teste de estresse: N escritores mudam preços em paralelo e medimos quanto
// tempo cada mudança leva para sair em changes$ (o ponto onde o WebSocket vai
// se ligar). Roda em um banco próprio, com a API em processo (a partir de dist/).
// Uso: npm run stress -- <escritores> <mudanças no total>
import { Pool } from 'pg';

const writers = Number(process.argv[2] ?? 10);
const total = Number(process.argv[3] ?? 5000);
const PRODUCTS = 1000;

const databaseUrl =
  'postgres://postgres:postgres@localhost:5432/realtime_notifications_stress_test';
process.env.TEST_DATABASE_URL = databaseUrl;
process.env.DATABASE_URL = databaseUrl;
process.env.PRICE_CHANGES_CONSUMER = 'stress';

// Cria o banco, aplica o schema e limpa os dados (mesmo setup dos testes e2e).
const { default: setupDatabase } = await import('../test/test-database.ts');
await setupDatabase();

const pool = new Pool({ connectionString: databaseUrl, max: writers });
await pool.query(
  `insert into products (name, store, price)
   select 'produto ' || n, 'stress', 1 from generate_series(1, $1) n`,
  [PRODUCTS],
);

const { NestFactory } = await import('@nestjs/core');
const { AppModule } = await import('../dist/app.module.js');
const { PriceChangeListener } =
  await import('../dist/price-changes/price-change.listener.js');
// Sem logs: um log por evento no terminal distorceria a medição.
const app = await NestFactory.createApplicationContext(AppModule, {
  logger: ['error', 'warn'],
});

const sentAt = new Map<string, number>(); // "produto:preço" -> envio do UPDATE
const writeMs: number[] = [];
const endToEndMs: number[] = [];
let duplicates = 0;
let outOfOrder = 0;
let lastId = 0n;
const seen = new Set<string>();
let allReceived = () => {};
const done = new Promise<void>((resolve) => (allReceived = resolve));

app.get(PriceChangeListener).changes$.subscribe((change) => {
  const now = performance.now();
  if (seen.has(change.id)) {
    duplicates++;
    return;
  }
  seen.add(change.id);
  if (BigInt(change.id) < lastId) outOfOrder++;
  lastId = BigInt(change.id);

  const sent = sentAt.get(`${change.productId}:${change.newPrice}`);
  if (sent !== undefined) endToEndMs.push(now - sent);
  if (seen.size === total) allReceived();
});

console.log(`${writers} escritores, ${total} mudanças de preço...`);
const start = performance.now();
let next = 0;
await Promise.all(
  Array.from({ length: writers }, async () => {
    while (next < total) {
      const n = ++next;
      const productId = 1 + (n % PRODUCTS);
      const price = (1 + n / 100).toFixed(2); // único por mudança
      const sent = performance.now();
      sentAt.set(`${productId}:${price}`, sent);
      await pool.query(
        'update products set price = $1, updated_at = now() where id = $2',
        [price, productId],
      );
      writeMs.push(performance.now() - sent);
    }
  }),
);
const writeSeconds = (performance.now() - start) / 1000;

const timeout = new Promise<void>((resolve) => setTimeout(resolve, 60_000));
await Promise.race([done, timeout]);
const totalSeconds = (performance.now() - start) / 1000;

const percentile = (values: number[], p: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return (sorted[Math.ceil((p / 100) * sorted.length) - 1] ?? NaN).toFixed(1);
};
const summary = (values: number[]) =>
  `p50 ${percentile(values, 50)}  p95 ${percentile(values, 95)}  p99 ${percentile(values, 99)}  max ${percentile(values, 100)}`;

console.log(`
Escrita:    ${(total / writeSeconds).toFixed(0)} mudanças/s (${writeSeconds.toFixed(2)}s)
Entrega:    ${(seen.size / totalSeconds).toFixed(0)} eventos/s (${totalSeconds.toFixed(2)}s até o último)
UPDATE (ms):            ${summary(writeMs)}
UPDATE -> changes$ (ms): ${summary(endToEndMs)}
Recebidos: ${seen.size}/${total}   perdidos: ${total - seen.size}   repetidos: ${duplicates}   fora de ordem: ${outOfOrder}
`);

await app.close();
await pool.end();
process.exit(seen.size === total && outOfOrder === 0 ? 0 : 1);
