-- Schema idempotente: pode ser reaplicado a qualquer momento (npm run db:setup).

-- Estado atual: o preço de agora de cada produto em cada loja.
create table if not exists products (
  id         serial primary key,
  name       text not null,
  store      text not null,
  price      numeric(10,2) not null,
  updated_at timestamptz not null default now(),
  unique (name, store)
);

-- Log de eventos (só recebe inserts, feitos pelo trigger). Serve de histórico
-- de preços e de outbox: a API lê daqui, em ordem de id, o que ainda não viu.
create table if not exists price_changes (
  id         bigserial primary key,
  product_id integer not null,
  name       text not null,
  store      text not null,
  old_price  numeric(10,2) not null,
  new_price  numeric(10,2) not null,
  changed_at timestamptz not null default now()
);

-- O histórico não é apagado junto com o produto: um produto com eventos não
-- pode ser removido. (Recriada a cada aplicação para corrigir bancos antigos,
-- em que esta chave tinha "on delete cascade".)
alter table price_changes drop constraint if exists price_changes_product_id_fkey;
alter table price_changes
  add constraint price_changes_product_id_fkey
  foreign key (product_id) references products (id);

-- Até onde cada consumidor já leu de price_changes.
create table if not exists consumer_cursors (
  consumer   text primary key,
  last_id    bigint not null default 0,
  updated_at timestamptz not null default now()
);

-- Grava a mudança em price_changes na mesma transação do UPDATE e avisa os
-- ouvintes do canal "price_changed". O NOTIFY é só um sinal de "há eventos
-- novos"; o dado é lido da tabela.
create or replace function record_price_change() returns trigger as $$
declare
  change_id bigint;
begin
  -- Serializa as transações que mudam preço, para que a ordem dos ids seja a
  -- ordem de commit. Sem isso, um leitor poderia ver o id 6 antes de o id 5
  -- ser commitado e avançar o cursor pulando o 5.
  perform pg_advisory_xact_lock(hashtext('price_changes'));

  insert into price_changes (product_id, name, store, old_price, new_price)
  values (new.id, new.name, new.store, old.price, new.price)
  returning id into change_id;

  perform pg_notify('price_changed', change_id::text);
  return new;
end;
$$ language plpgsql;

drop trigger if exists products_price_changed on products;
drop function if exists notify_price_changed();

create or replace trigger products_record_price_change
  after update of price on products
  for each row
  when (old.price is distinct from new.price)
  execute function record_price_change();
