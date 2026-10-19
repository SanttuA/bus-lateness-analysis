import { describe, expect, it } from 'vitest';

import {
  formatAlertPriority,
  formatDate,
  formatDuration,
  formatInterval,
  formatMinutes,
  formatNumber,
  formatPercent,
  formatSigned,
} from '../format';

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

  it('renders a missing alert priority as unknown instead of a high-priority number', () => {
    expect(formatAlertPriority(-1, 'unknown')).toBe('unknown');
    expect(formatAlertPriority(0, 'unknown')).toBe('0');
    expect(formatAlertPriority(1000, 'unknown')).toBe('1000');
  });

  it('formats trip-level intervals and signed differences', () => {
    expect(formatInterval(16.61, 20.6, 'fi', '%')).toBe(' (95 %: 16,6–20,6 %)');
    expect(formatInterval(1.75, 2.59, 'en', 'pp')).toBe(' (95%: 1.8–2.6 pp)');
    expect(formatInterval(null, 2, 'en', 'min')).toBe('');
    expect(formatSigned(2.16, 'en')).toBe('+2.2');
    expect(formatSigned(-0.4, 'fi')).toBe('−0,4');
    expect(formatSigned(0, 'en')).toBe('0');
  });
});
