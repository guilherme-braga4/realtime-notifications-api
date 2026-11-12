/** Canal do Postgres (LISTEN/NOTIFY) que avisa que há eventos novos. */
export const PRICE_CHANGED_CHANNEL = 'price_changed';

/** Um evento da tabela `price_changes`. */
export interface PriceChange {
  /** Id do evento (bigint, como texto). Cresce na ordem em que os eventos ocorreram. */
  id: string;
  productId: number;
  name: string;
  store: string;
  /** Preços como texto decimal (ex.: "4796.53") para não perder precisão. */
  oldPrice: string;
  newPrice: string;
  /** Instante da mudança, em ISO 8601. */
  changedAt: string;
}
