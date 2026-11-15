/** Canal do Postgres (LISTEN/NOTIFY) que avisa que há eventos novos. */
export const EVENTS_CHANNEL = 'events';

/** Tópicos entregues aos clientes. Cada um é uma tabela em `db/schema.sql`. */
export const TOPICS = ['prices', 'metrics', 'stocks', 'news'] as const;
export type Topic = (typeof TOPICS)[number];

/** Um item de um tópico. O formato de cada tópico é definido em `db/schema.sql`. */
export type Item = Record<string, unknown>;

/** Um evento da tabela `events`: um item de um tópico foi criado ou alterado. */
export interface TopicEvent {
  /** Id do evento (bigint, como texto). Cresce na ordem em que os eventos ocorreram. */
  id: string;
  topic: string;
  /** Identifica o item dentro do tópico. */
  key: string;
  /** O item depois da mudança, no mesmo formato do snapshot. */
  item: Item;
  /** O item antes da mudança; `null` quando acabou de ser criado. */
  previous: Item | null;
  /** Instante do evento, em ISO 8601. */
  at: string;
}
