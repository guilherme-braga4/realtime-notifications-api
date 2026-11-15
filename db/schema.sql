-- Schema idempotente: pode ser reaplicado a qualquer momento (npm run db:setup).
--
-- Cada tópico do site é uma tabela de estado atual. Toda linha criada ou
-- alterada nelas vira um evento na tabela `events` (outbox), na mesma
-- transação, e a API entrega esses eventos por WebSocket.
--
--   tópico    tabela     chave do item
--   prices    products   id
--   metrics   metrics    key
--   stocks    stocks     symbol
--   news      news       id

-- ── Estado atual ────────────────────────────────────────────────────────────

-- Preço de agora de cada produto em cada loja.
create table if not exists products (
  id         serial primary key,
  name       text not null,
  store      text not null,
  price      numeric(10,2) not null,
  updated_at timestamptz not null default now(),
  unique (name, store)
);

-- Indicadores (energia, água, tokens, chips...): um número por chave.
create table if not exists metrics (
  key        text primary key,
  label      text not null,
  unit       text not null,
  value      numeric not null,
  updated_at timestamptz not null default now()
);

-- Cotação de agora de cada ação.
create table if not exists stocks (
  symbol         text primary key,
  name           text not null,
  price          numeric(12,2) not null,
  previous_close numeric(12,2) not null,
  updated_at     timestamptz not null default now()
);

create table if not exists news (
  id           bigserial primary key,
  title        text not null,
  url          text not null,
  source       text not null,
  published_at timestamptz not null default now()
);

-- ── Outbox ──────────────────────────────────────────────────────────────────

-- Log de eventos (só recebe inserts, feitos pelos triggers). A API lê daqui,
-- em ordem de id, o que ainda não viu. `item` é o estado do item depois da
-- mudança; `previous` é o estado anterior (null quando o item acabou de nascer).
create table if not exists events (
  id         bigserial primary key,
  topic      text not null,
  key        text not null,
  item       jsonb not null,
  previous   jsonb,
  created_at timestamptz not null default now()
);

-- Até onde cada consumidor já leu de `events`.
create table if not exists consumer_cursors (
  consumer   text primary key,
  last_id    bigint not null default 0,
  updated_at timestamptz not null default now()
);

-- ── Formato de cada item ────────────────────────────────────────────────────
-- Usado pelo trigger (evento) e pela API (snapshot): os dois saem iguais.
-- Números vão como texto decimal para não perder precisão no cliente.

create or replace function price_item(p products) returns jsonb
language sql stable as $$
  select jsonb_build_object(
    'id', p.id, 'name', p.name, 'store', p.store,
    'price', p.price::text, 'updatedAt', p.updated_at)
$$;

create or replace function metric_item(m metrics) returns jsonb
language sql stable as $$
  select jsonb_build_object(
    'key', m.key, 'label', m.label, 'unit', m.unit,
    'value', m.value::text, 'updatedAt', m.updated_at)
$$;

create or replace function stock_item(s stocks) returns jsonb
language sql stable as $$
  select jsonb_build_object(
    'symbol', s.symbol, 'name', s.name, 'price', s.price::text,
    'previousClose', s.previous_close::text, 'updatedAt', s.updated_at)
$$;

create or replace function news_item(n news) returns jsonb
language sql stable as $$
  select jsonb_build_object(
    'id', n.id, 'title', n.title, 'url', n.url,
    'source', n.source, 'publishedAt', n.published_at)
$$;

-- ── Publicação ──────────────────────────────────────────────────────────────

-- Grava o evento na mesma transação da mudança e avisa os ouvintes do canal
-- "events". O NOTIFY é só um sinal de "há eventos novos"; o dado é lido da
-- tabela.
create or replace function publish_event(
  p_topic text, p_key text, p_item jsonb, p_previous jsonb
) returns void as $$
declare
  event_id bigint;
begin
  -- Nada mudou além da data de atualização: não é um evento.
  if p_item - 'updatedAt' = p_previous - 'updatedAt' then
    return;
  end if;

  -- Serializa as transações que publicam, para que a ordem dos ids seja a
  -- ordem de commit. Sem isso, um leitor poderia ver o id 6 antes de o id 5
  -- ser commitado e avançar o cursor pulando o 5.
  perform pg_advisory_xact_lock(hashtext('events'));

  insert into events (topic, key, item, previous)
  values (p_topic, p_key, p_item, p_previous)
  returning id into event_id;

  perform pg_notify('events', event_id::text);
end;
$$ language plpgsql;

create or replace function publish_price() returns trigger as $$
begin
  perform publish_event('prices', new.id::text, price_item(new),
    case when tg_op = 'UPDATE' then price_item(old) end);
  return null;
end;
$$ language plpgsql;

create or replace function publish_metric() returns trigger as $$
begin
  perform publish_event('metrics', new.key, metric_item(new),
    case when tg_op = 'UPDATE' then metric_item(old) end);
  return null;
end;
$$ language plpgsql;

create or replace function publish_stock() returns trigger as $$
begin
  perform publish_event('stocks', new.symbol, stock_item(new),
    case when tg_op = 'UPDATE' then stock_item(old) end);
  return null;
end;
$$ language plpgsql;

create or replace function publish_news() returns trigger as $$
begin
  perform publish_event('news', new.id::text, news_item(new),
    case when tg_op = 'UPDATE' then news_item(old) end);
  return null;
end;
$$ language plpgsql;

create or replace trigger products_publish
  after insert or update on products
  for each row execute function publish_price();

create or replace trigger metrics_publish
  after insert or update on metrics
  for each row execute function publish_metric();

create or replace trigger stocks_publish
  after insert or update on stocks
  for each row execute function publish_stock();

create or replace trigger news_publish
  after insert or update on news
  for each row execute function publish_news();

-- ── Bancos antigos ──────────────────────────────────────────────────────────
-- Antes de `events`, só preços geravam eventos, em `price_changes`. Os
-- triggers antigos saem; a tabela `price_changes` fica como está, sem uso.
drop trigger if exists products_record_price_change on products;
drop trigger if exists products_price_changed on products;
drop function if exists record_price_change();
drop function if exists notify_price_changed();
