// Simula os dados do site: altera preços, indicadores e ações no Postgres e
// publica notícias. TODOS os valores são fictícios, só para ver o tempo real.
// Uso: npm run simulate (as tabelas vêm de db/schema.sql: npm run db:setup)
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const stores = ['Mercado Livre', 'Amazon', 'Shopee', 'Magalu'];
const items: [string, number][] = [
  ['iPhone 16 128GB', 5200],
  ['Galaxy S25 256GB', 4300],
  ['PlayStation 5 Slim', 3500],
  ['Smart TV 55" 4K', 2600],
  ['Notebook Ryzen 7 16GB', 4100],
  ['Air Fryer 5L', 420],
  ['Kindle 11ª geração', 480],
  ['Fone Bluetooth ANC', 650],
];
const metrics: [string, string, string, number][] = [
  ['energy', 'Energia consumida por IA', 'TWh', 400],
  ['water', 'Água consumida por IA', 'bilhões de litros', 700],
  ['ai_chips', 'Chips de IA produzidos', 'unidades', 4_000_000],
  ['claude_tokens', 'Tokens consumidos pelo Claude', 'trilhões', 900],
  ['it_unemployment', 'Desemprego em TI', '%', 5],
  ['ai_layoffs', 'Demissões atribuídas à IA', 'pessoas', 150_000],
];
const stocks: [string, string, number][] = [
  ['NVDA', 'Nvidia', 110],
  ['AMD', 'AMD', 160],
  ['TSM', 'TSMC', 190],
  ['MSFT', 'Microsoft', 430],
  ['GOOGL', 'Alphabet', 170],
  ['META', 'Meta', 560],
  ['AVGO', 'Broadcom', 170],
  ['ASML', 'ASML', 700],
];

for (const [name, basePrice] of items) {
  for (const store of stores) {
    const price = basePrice * (0.95 + Math.random() * 0.1);
    await pool.query(
      `insert into products (name, store, price) values ($1, $2, $3)
       on conflict (name, store) do nothing`,
      [name, store, price.toFixed(2)],
    );
  }
}
for (const metric of metrics) {
  await pool.query(
    `insert into metrics (key, label, unit, value) values ($1, $2, $3, $4)
     on conflict (key) do nothing`,
    metric,
  );
}
for (const stock of stocks) {
  await pool.query(
    `insert into stocks (symbol, name, price, previous_close) values ($1, $2, $3, $3)
     on conflict (symbol) do nothing`,
    stock,
  );
}

console.log('Simulador rodando. Ctrl+C para parar.');

for (let tick = 1; ; tick++) {
  // Rajada: de 1 a 5 produtos mudam de preço em até 10%, para cima ou para baixo.
  const burst = 1 + Math.floor(Math.random() * 5);
  const prices = await pool.query(
    `update products
        set price = round(price * (0.9 + random() * 0.2)::numeric, 2), updated_at = now()
      where id in (select id from products order by random() limit $1)
  returning name, store, price`,
    [burst],
  );
  for (const row of prices.rows) {
    console.log(`preço    ${row.store.padEnd(14)} ${row.name.padEnd(24)} R$ ${row.price}`);
  }

  // Um indicador sobe até 0,1% e duas ações variam até 1%.
  const metric = await pool.query(
    `update metrics
        set value = round(value * (1 + random() * 0.001)::numeric, 4), updated_at = now()
      where key = (select key from metrics order by random() limit 1)
  returning label, value, unit`,
  );
  for (const row of metric.rows) {
    console.log(`métrica  ${row.label}: ${row.value} ${row.unit}`);
  }
  const quotes = await pool.query(
    `update stocks
        set price = round(price * (0.99 + random() * 0.02)::numeric, 2), updated_at = now()
      where symbol in (select symbol from stocks order by random() limit 2)
  returning symbol, price`,
  );
  for (const row of quotes.rows) {
    console.log(`ação     ${row.symbol} US$ ${row.price}`);
  }

  // Uma notícia a cada 10 rodadas, em média.
  if (Math.random() < 0.1) {
    const title = `Notícia simulada nº ${tick}`;
    await pool.query(
      `insert into news (title, url, source) values ($1, 'https://example.com/noticia', 'Simulador')`,
      [title],
    );
    console.log(`notícia  ${title}`);
  }

  await new Promise((resolve) => setTimeout(resolve, 500 + Math.random() * 2500));
}
