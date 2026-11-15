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
npm run start:dev             # WebSocket em ws://localhost:3000/ws
npm test                      # testes unitários
npm run test:e2e              # testes e2e (precisam do Postgres no ar)
```

Os testes e2e usam um banco próprio, `realtime_notifications_test`, criado e limpo automaticamente a cada execução. Eles não tocam no banco da API local.

Ao subir, a API registra no log a conexão com o banco:

```
[DatabaseService] Connected to Postgres 18.6 at localhost:5432/realtime_notifications as "postgres"
```

## Tópicos e captura de mudanças

Cada tópico é uma tabela com o estado atual. Toda linha criada ou alterada vira um evento, na mesma transação.

| Tópico | Tabela | Chave | Item |
|---|---|---|---|
| `prices` | `products` | `id` | `id`, `name`, `store`, `price`, `updatedAt` |
| `metrics` | `metrics` | `key` | `key`, `label`, `unit`, `value`, `updatedAt` |
| `stocks` | `stocks` | `symbol` | `symbol`, `name`, `price`, `previousClose`, `updatedAt` |
| `news` | `news` | `id` | `id`, `title`, `url`, `source`, `publishedAt` |

Números (`price`, `value`, `previousClose`) vão como texto decimal, para não perder precisão.

```
INSERT ou UPDATE em uma tabela de tópico
  └─ trigger (mesma transação)
       ├─ INSERT em events               ← o evento, durável
       └─ NOTIFY events                  ← só o aviso "há eventos novos"
            └─ EventsListener lê events depois do cursor → events$
```

| Tabela | Papel | Como muda |
|---|---|---|
| `products`, `metrics`, `stocks`, `news` | Estado atual de cada tópico | `insert` e `update` |
| `events` | Log de eventos (outbox) de todos os tópicos | Só `insert`, feito pelos triggers |
| `consumer_cursors` | Até onde cada consumidor leu | `update` de uma linha por consumidor |

- **Sem broker:** o Postgres faz o papel de Pub/Sub (LISTEN/NOTIFY) e de outbox (`events`).
- **Um formato por tópico, em um lugar só:** as funções `price_item`, `metric_item`, `stock_item` e `news_item` (`db/schema.sql`) montam o item tanto para o evento quanto para o snapshot.
- **Só mudança de verdade vira evento:** um `update` que altera apenas a data de atualização não grava nada.
- **Nada se perde em quedas:** ao conectar ou reconectar (backoff de 1s a 30s), o listener lê tudo o que está depois do cursor.
- **Ordem garantida, entre todos os tópicos:** os triggers serializam as transações que publicam com um advisory lock, para que a ordem dos ids seja a ordem de commit. O custo é que eventos não são gravados em paralelo.
- **Entrega "pelo menos uma vez":** se a API cair entre emitir um lote e salvar o cursor, o lote é reemitido ao reiniciar. Consumidores devem usar o `id` do evento para ignorar repetidos.
- **Primeira execução:** um consumidor novo começa no último evento existente; o histórico anterior não é reenviado.
- O nome do consumidor vem de `EVENTS_CONSUMER` (padrão: `events-listener`).

O schema (`db/schema.sql`) é aplicado automaticamente quando o volume do Postgres é criado. Em um banco já existente:

```bash
npm run db:setup
```

Para ver os eventos gravados:

```sql
select * from events order by id desc limit 20;
select * from consumer_cursors;
```

Para criar um tópico: uma tabela, uma função `*_item`, um trigger que chama `publish_event` (tudo em `db/schema.sql`), o nome em `TOPICS` (`src/events/topic-event.ts`) e a consulta do snapshot em `EventsGateway`.

## WebSocket

O gateway fica em `ws://localhost:3000/ws` e só envia mensagens (JSON). Toda mensagem traz o `topic`.

```jsonc
// ao conectar: um snapshot por tópico, com os itens atuais
{ "type": "snapshot", "topic": "stocks", "lastEventId": "489", "items": [{ "symbol": "NVDA", "name": "Nvidia", "price": "113.40", "previousClose": "110.00", "updatedAt": "..." }] }
// a cada item criado ou alterado, em ordem
{ "type": "event", "topic": "stocks", "id": "490", "key": "NVDA", "item": { "symbol": "NVDA", "name": "Nvidia", "price": "98.70", "previousClose": "110.00", "updatedAt": "..." }, "previous": { "symbol": "NVDA", "price": "113.40", "...": "..." }, "at": "..." }
```

- **Como o cliente aplica um evento:** troca (ou inclui) o item do tópico que tem aquela `key`. O `item` do evento tem o mesmo formato dos itens do snapshot; `previous` é o item antes da mudança, ou `null` se acabou de ser criado.
- **Sem buraco nem repetição entre snapshots e fluxo:** os snapshots de todos os tópicos saem de uma única leitura do banco, com o mesmo `lastEventId`. O cliente é registrado antes dessa leitura, os eventos que chegam nesse intervalo ficam em fila, e só passam os com `id` maior que o `lastEventId`.
- **Notícias:** o snapshot traz as 50 mais recentes.
- **Reconexão:** o cliente reconecta e recebe snapshots novos, que repõem o estado; não há reenvio de eventos perdidos por cliente.
- **Clientes lentos ou mortos:** quem acumula 1 MB sem ler é desconectado, e um ping a cada 30s derruba conexões que não respondem.
- Ainda não há autenticação nem filtro por tópico: todo cliente recebe todos os eventos.

A tela que consome este WebSocket fica em outro repositório (`bolha-da-ia-web`).

## Simulador

Enquanto não há fontes reais, um script altera preços, indicadores e ações e publica notícias. Todos os valores são fictícios.

```bash
npm run simulate   # Ctrl+C para parar
```

Com a API no ar, cada evento aparece no log:

```
[EventsListener] Event 490: stocks/NVDA
```

## Teste de estresse

Escritores em paralelo mudam preços e o script mede quanto tempo cada mudança leva do `UPDATE` até sair em `events$`. Roda em um banco próprio (`realtime_notifications_stress_test`), com a API em processo e sem logs por evento.

```bash
npm run stress -- 50 20000   # 50 escritores, 20.000 mudanças no total
```

Resultados em 12/11/2026 (MacBook de 10 núcleos, Postgres 18 no Docker Desktop, uma execução por cenário). São de antes do modelo por tópicos, quando só preços geravam eventos; a tabela ainda não foi refeita:

| Escritores | Mudanças/s | UPDATE p50 / p99 | UPDATE → listener p50 / p99 | Perdidos / fora de ordem |
|---|---|---|---|---|
| 1 | 598 | 0,8 / 11,7 ms | 1,5 / 19,2 ms | 0 / 0 |
| 10 | 1.091 | 7,0 / 39,5 ms | 8,0 / 43,7 ms | 0 / 0 |
| 50 | 1.239 | 32,0 / 149,1 ms | 32,9 / 152,8 ms | 0 / 0 |
| 90 | 947 | 61,7 / 547,4 ms | 62,6 / 548,4 ms | 0 / 0 |

- A entrega acrescenta cerca de 1 ms ao tempo da própria escrita, em qualquer carga.
- A vazão para de crescer por volta de 1.200 mudanças/s: mais escritores só aumentam a fila. O gargalo está na escrita, não no listener.

## Status

🚧 Em fase inicial: preços, indicadores, ações e notícias simulados chegam ao navegador em tempo real. Ainda não há fontes reais, autenticação nem deploy.
