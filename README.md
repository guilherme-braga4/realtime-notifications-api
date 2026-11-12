# realtime-notifications-api

Backend real-time com Pub/Sub + WebSocket.

## Objetivo

Notificar clientes conectados, em tempo real, sempre que um evento no banco de dados é atualizado.

```
Postgres --(trigger + outbox + NOTIFY)--> API --> WebSocket --> Navegador
```

## Contexto

- Projeto de estudos sobre arquitetura orientada a eventos e comunicação em tempo real.
- Vai evoluir para a API de um App.

## Stack

- NestJS 12 + TypeScript (ESM)
- Vitest (testes unitários e e2e)
- PostgreSQL 18 (Docker), acessado com `pg`
- Pub/Sub: LISTEN/NOTIFY do Postgres (sem broker)
- WebSocket: `@nestjs/websockets` com `ws`

## Como rodar

Requer Node.js 24+ e Docker.

```bash
cp .env.example .env
docker compose up -d --wait   # Postgres em localhost:5432
npm install
npm run start:dev             # http://localhost:3000 (página de preços)
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

## WebSocket e página de preços

Abra `http://localhost:3000` com a API no ar e rode `npm run simulate`: a tabela mostra o preço de cada produto por loja, destaca o menor e pisca a cada mudança.

O gateway fica em `ws://localhost:3000/ws` e só envia mensagens (JSON):

```jsonc
// ao conectar: preço atual de todos os produtos
{ "type": "snapshot", "lastEventId": "489", "products": [{ "id": 23, "name": "Air Fryer 5L", "store": "Shopee", "price": "370.81", "updatedAt": "..." }] }
// a cada mudança, em ordem
{ "type": "price_change", "change": { "id": "490", "productId": 23, "name": "Air Fryer 5L", "store": "Shopee", "oldPrice": "370.81", "newPrice": "352.10", "changedAt": "..." } }
```

- **Sem buraco nem repetição entre snapshot e fluxo:** o cliente é registrado antes da leitura do snapshot, as mudanças que chegam nesse intervalo ficam em fila, e só passam as com `id` maior que o `lastEventId` do snapshot.
- **Reconexão:** o cliente reconecta e recebe um snapshot novo, que repõe o estado; não há reenvio de eventos perdidos por cliente.
- **Clientes lentos ou mortos:** quem acumula 1 MB sem ler é desconectado, e um ping a cada 30s derruba conexões que não respondem.
- Ainda não há autenticação nem filtro por produto: todo cliente recebe todas as mudanças.

## Simulador de preços

Enquanto não há integração com lojas reais, um script simula a Black Friday alterando preços em rajadas na tabela `products`.

```bash
npm run simulate   # Ctrl+C para parar
```

Com a API no ar, cada mudança aparece no log:

```
[PriceChangeListener] Price changed: [Shopee] Air Fryer 5L 379.83 -> 370.81 (event 463, product 23)
```

## Teste de estresse

Escritores em paralelo mudam preços e o script mede quanto tempo cada mudança leva do `UPDATE` até sair em `changes$`. Roda em um banco próprio (`realtime_notifications_stress_test`), com a API em processo e sem logs por evento.

```bash
npm run stress -- 50 20000   # 50 escritores, 20.000 mudanças no total
```

Resultados em 12/11/2026 (MacBook de 10 núcleos, Postgres 18 no Docker Desktop, uma execução por cenário):

| Escritores | Mudanças/s | UPDATE p50 / p99 | UPDATE → `changes$` p50 / p99 | Perdidos / fora de ordem |
|---|---|---|---|---|
| 1 | 598 | 0,8 / 11,7 ms | 1,5 / 19,2 ms | 0 / 0 |
| 10 | 1.091 | 7,0 / 39,5 ms | 8,0 / 43,7 ms | 0 / 0 |
| 50 | 1.239 | 32,0 / 149,1 ms | 32,9 / 152,8 ms | 0 / 0 |
| 90 | 947 | 61,7 / 547,4 ms | 62,6 / 548,4 ms | 0 / 0 |

- A entrega acrescenta cerca de 1 ms ao tempo da própria escrita, em qualquer carga.
- A vazão para de crescer por volta de 1.200 mudanças/s: mais escritores só aumentam a fila. O gargalo está na escrita, não no listener.

## Status

🚧 Em fase inicial: mudanças de preço simuladas chegam ao navegador em tempo real. Ainda não há lojas reais, autenticação nem deploy.
