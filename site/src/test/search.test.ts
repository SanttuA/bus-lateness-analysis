import { describe, expect, it } from 'vitest';

import { normalizeHourRange, validateSearch } from '../search';

describe('validateSearch', () => {
  it('keeps valid shareable filters', () => {
    expect(
      validateSearch({
        line: '612',
        direction: '2',
        day: 'weekend',
        from: '7',
        to: 18,
        stop: 'Kauppatori',
        metric: 'early',
        view: 'table',
      }),
    ).toEqual({
      line: '612',
      direction: '2',
      day: 'weekend',
      from: 7,
      to: 18,
      stop: 'Kauppatori',
      metric: 'early',
      view: 'table',
    });
  });

  it('drops invalid values', () => {
    expect(validateSearch({ direction: '9', day: 'all', from: -1, to: 24 })).toEqual({});
  });

  it('preserves numeric-looking route and direction values decoded by URL parsers', () => {
    expect(validateSearch({ line: 24, direction: 1, stop: 1234 })).toEqual({
      line: '24',
      direction: '1',
      stop: '1234',
    });
  });
});

describe('normalizeHourRange', () => {
  it('uses the full day by default', () => {
    expect(normalizeHourRange(undefined, undefined)).toEqual({ start: 0, end: 23 });
  });

  it('orders reversed bounds', () => {
    expect(normalizeHourRange(18, 7)).toEqual({ start: 7, end: 18 });
  });
});
