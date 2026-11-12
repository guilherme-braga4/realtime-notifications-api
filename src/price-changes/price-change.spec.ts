import { parsePriceChange } from './price-change.js';

const valid = {
  productId: 7,
  name: 'Air Fryer 5L',
  store: 'Shopee',
  oldPrice: '420.00',
  newPrice: '398.70',
  changedAt: '2026-11-27T03:00:00.123456+00:00',
};

describe('parsePriceChange', () => {
  it('parses a valid payload', () => {
    expect(parsePriceChange(JSON.stringify(valid))).toEqual(valid);
  });

  it('drops unknown fields', () => {
    const parsed = parsePriceChange(JSON.stringify({ ...valid, extra: 1 }));
    expect(parsed).toEqual(valid);
  });

  it.each([
    ['empty payload', undefined],
    ['non-JSON payload', 'not json'],
    ['non-object payload', '42'],
    ['null payload', 'null'],
    ['missing productId', JSON.stringify({ ...valid, productId: undefined })],
    ['non-integer productId', JSON.stringify({ ...valid, productId: 1.5 })],
    ['non-string name', JSON.stringify({ ...valid, name: 1 })],
    ['numeric price', JSON.stringify({ ...valid, newPrice: 398.7 })],
    ['non-decimal price', JSON.stringify({ ...valid, oldPrice: 'abc' })],
    ['invalid date', JSON.stringify({ ...valid, changedAt: 'yesterday' })],
  ])('rejects %s', (_case, payload) => {
    expect(() => parsePriceChange(payload)).toThrow();
  });
});
