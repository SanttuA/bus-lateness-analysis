import { describe, expect, it } from 'vitest';

import { formatDate, formatDuration, formatMinutes, formatNumber, formatPercent } from '../format';

describe('reader-facing formatting', () => {
  it('uses localized separators and units', () => {
    expect(formatNumber(3746770, 'en')).toBe('3,746,770');
    expect(formatMinutes(2.875, 'en', 2)).toBe('2.88 min');
    expect(formatPercent(5.688, 'fi', 2)).toBe('5,69 %');
  });

  it('formats long collection gaps in hours', () => {
    expect(formatDuration(180, 'en')).toBe('3 h');
    expect(formatDuration(45, 'en')).toBe('45 min');
  });

  it('formats reader-facing snapshot dates', () => {
    expect(formatDate('2026-05-23', 'en')).toContain('2026');
  });
});
