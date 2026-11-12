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

Os testes e2e usam um banco próprio, `realtime_notifications_test`, criado e limpo automaticamente a cada execução. Eles não tocam no banco da API local.

Ao subir, a API registra no log a conexão com o banco:

```
[DatabaseService] Connected to Postgres 18.6 at localhost:5432/realtime_notifications as "postgres"
```

## Captura de mudanças de preço

```
UPDATE em products
  └─ trigger (mesma transação)
       ├─ INSERT em price_changes        ← o evento, durável
       └─ NOTIFY price_changed           ← só o aviso "há eventos novos"
            └─ PriceChangeListener lê price_changes depois do cursor → changes$
```

| Tabela | Papel | Como muda |
|---|---|---|
| `products` | Estado atual (preço de agora) | `update` a cada mudança |
| `price_changes` | Log de eventos e histórico de preços | Só `insert`, feito pelo trigger |
| `consumer_cursors` | Até onde cada consumidor leu | `update` de uma linha por consumidor |

- **Sem broker:** o Postgres faz o papel de Pub/Sub (LISTEN/NOTIFY) e de outbox (`price_changes`).
- **Nada se perde em quedas:** ao conectar ou reconectar (backoff de 1s a 30s), o listener lê tudo o que está depois do cursor.
- **Ordem garantida:** o trigger serializa as transações que mudam preço com um advisory lock, para que a ordem dos ids seja a ordem de commit. O custo é que mudanças de preço não são gravadas em paralelo.
- **Entrega "pelo menos uma vez":** se a API cair entre emitir um lote e salvar o cursor, o lote é reemitido ao reiniciar. Consumidores devem usar o `id` do evento para ignorar repetidos.
- **Primeira execução:** um consumidor novo começa no último evento existente; o histórico anterior não é reenviado.
- O nome do consumidor vem de `PRICE_CHANGES_CONSUMER` (padrão: `price-change-listener`).

O schema (`db/schema.sql`) é aplicado automaticamente quando o volume do Postgres é criado. Em um banco já existente:

```bash
npm run db:setup
```

Para ver os eventos gravados:

```sql
select * from price_changes order by id desc limit 20;
select * from consumer_cursors;
```

## Simulador de preços

Enquanto não há integração com lojas reais, um script simula a Black Friday alterando preços em rajadas na tabela `products`.

```bash
npm run simulate   # Ctrl+C para parar
```

Com a API no ar, cada mudança aparece no log:

```
[PriceChangeListener] Price changed: [Shopee] Air Fryer 5L 379.83 -> 370.81 (event 463, product 23)
```

## Status

🚧 Em fase inicial: a API captura mudanças de preço do Postgres. Pub/Sub e WebSocket ainda não existem.
