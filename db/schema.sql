-- Schema idempotente: pode ser reaplicado a qualquer momento (npm run db:setup).

create table if not exists products (
  id         serial primary key,
  name       text not null,
  store      text not null,
  price      numeric(10,2) not null,
  updated_at timestamptz not null default now(),
  unique (name, store)
);

-- Publica cada mudança de preço no canal "price_changed" (LISTEN/NOTIFY).
-- Os preços vão como texto para não perder precisão no JSON.
create or replace function notify_price_changed() returns trigger as $$
begin
  perform pg_notify(
    'price_changed',
    json_build_object(
      'productId', new.id,
      'name',      new.name,
      'store',     new.store,
      'oldPrice',  old.price::text,
      'newPrice',  new.price::text,
      'changedAt', new.updated_at
    )::text
  );
  return new;
end;
$$ language plpgsql;

create or replace trigger products_price_changed
  after update of price on products
  for each row
  when (old.price is distinct from new.price)
  execute function notify_price_changed();
