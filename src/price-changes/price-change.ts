/** Canal do Postgres (LISTEN/NOTIFY) onde o trigger de `products` publica. */
export const PRICE_CHANGED_CHANNEL = 'price_changed';

/** Mudança de preço de um produto, como publicada pelo trigger do banco. */
export interface PriceChange {
  productId: number;
  name: string;
  store: string;
  /** Preços como texto decimal (ex.: "4796.53") para não perder precisão. */
  oldPrice: string;
  newPrice: string;
  /** Instante da mudança, em ISO 8601. */
  changedAt: string;
}

const PRICE_PATTERN = /^\d+(\.\d+)?$/;

/**
 * Converte o payload de uma notificação em `PriceChange`.
 * Lança erro se o payload não for JSON ou não tiver o formato esperado.
 */
export function parsePriceChange(payload: string | undefined): PriceChange {
  if (!payload) {
    throw new Error('empty payload');
  }

  const data: unknown = JSON.parse(payload);
  if (typeof data !== 'object' || data === null) {
    throw new Error('payload is not an object');
  }

  const { productId, name, store, oldPrice, newPrice, changedAt } =
    data as Record<string, unknown>;

  if (!Number.isInteger(productId)) {
    throw new Error('invalid productId');
  }
  if (typeof name !== 'string' || typeof store !== 'string') {
    throw new Error('invalid name or store');
  }
  if (
    typeof oldPrice !== 'string' ||
    typeof newPrice !== 'string' ||
    !PRICE_PATTERN.test(oldPrice) ||
    !PRICE_PATTERN.test(newPrice)
  ) {
    throw new Error('invalid price');
  }
  if (typeof changedAt !== 'string' || Number.isNaN(Date.parse(changedAt))) {
    throw new Error('invalid changedAt');
  }

  return {
    productId: productId as number,
    name,
    store,
    oldPrice,
    newPrice,
    changedAt,
  };
}
