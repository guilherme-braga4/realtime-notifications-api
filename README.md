# realtime-notifications-api

Backend real-time com Pub/Sub + WebSocket.

## Objetivo

Notificar clientes conectados, em tempo real, sempre que um evento no banco de dados é atualizado.

```
Banco de dados --(mudança)--> Pub/Sub --> API --> WebSocket --> Clientes
```

## Contexto

- Projeto de estudos sobre arquitetura orientada a eventos e comunicação em tempo real.
- Vai evoluir para a API de um App.

## Stack

- NestJS 12 + TypeScript (ESM)
- Vitest (testes unitários e e2e)
- PostgreSQL 18 (Docker), acessado com `pg`
- Pub/Sub e WebSocket: a definir

## Como rodar

Requer Node.js 24+ e Docker.

```bash
cp .env.example .env
docker compose up -d --wait   # Postgres em localhost:5432
npm install
npm run start:dev             # http://localhost:3000
npm test                      # testes unitários
npm run test:e2e              # testes e2e (precisam do Postgres no ar)
```

Ao subir, a API registra no log a conexão com o banco:

```
[DatabaseService] Connected to Postgres 18.6 at localhost:5432/realtime_notifications as "postgres"
```

## Captura de mudanças de preço

```
UPDATE em products --> trigger --> pg_notify('price_changed') --> PriceChangeListener --> changes$
```

- `db/schema.sql` cria a tabela `products` e um trigger que publica cada mudança de preço no canal `price_changed` (LISTEN/NOTIFY).
- `PriceChangeListener` escuta o canal em uma conexão dedicada, valida o payload, registra no log e expõe as mudanças no observable `changes$`.
- Se a conexão cair, o listener reconecta com backoff exponencial (1s a 30s). LISTEN/NOTIFY não guarda mensagens: o que for publicado durante a queda não é entregue.

O schema é aplicado automaticamente quando o volume do Postgres é criado. Em um banco já existente:

```bash
npm run db:setup
```

## Simulador de preços

Enquanto não há integração com lojas reais, um script simula a Black Friday alterando preços em rajadas na tabela `products`.

```bash
npm run simulate   # Ctrl+C para parar
```

Com a API no ar, cada mudança aparece no log:

```
[PriceChangeListener] Price changed: [Shopee] Air Fryer 5L 398.70 -> 379.83 (product 23)
```

## Status

🚧 Em fase inicial: a API captura mudanças de preço do Postgres. Pub/Sub e WebSocket ainda não existem.
