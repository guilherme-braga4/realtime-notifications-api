// Simula a Black Friday: altera preços no Postgres em rajadas.
// Uso: npm run simulate (a tabela products vem de db/schema.sql: npm run db:setup)
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

console.log('Simulador rodando. Ctrl+C para parar.');

for (;;) {
  // Rajada: de 1 a 5 produtos mudam de preço em até 10%, para cima ou para baixo.
  const burst = 1 + Math.floor(Math.random() * 5);
  const { rows } = await pool.query(
    `update products
        set price = round(price * (0.9 + random() * 0.2)::numeric, 2), updated_at = now()
      where id in (select id from products order by random() limit $1)
  returning name, store, price`,
    [burst],
  );
  for (const row of rows) {
    console.log(`${row.store.padEnd(14)} ${row.name.padEnd(24)} R$ ${row.price}`);
  }
  await new Promise((resolve) => setTimeout(resolve, 500 + Math.random() * 2500));
}
