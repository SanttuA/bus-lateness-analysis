import { describe, expect, it } from 'vitest';

import { dataLabel, t } from '../i18n';

describe('reader-facing labels', () => {
  it('uses concise explorer labels in both languages', () => {
    expect(t('fi').stopSearch).toBe('Pysäkin nimi tai tunnus');
    expect(t('en').stopSearch).toBe('Stop name or ID');
    expect(t('fi').alertPriority).toBe('Tiedoteprioriteetti');
    expect(t('en').alertPriorityDescription).toContain('smaller priority number');
  });

  it('translates public data codes instead of exposing raw identifiers', () => {
    expect(dataLabel('fi', 'alertCause', 'other_cause')).toBe('Muu syy');
    expect(dataLabel('fi', 'alertEffect', 'significant_delays')).toBe('Merkittäviä viiveitä');
    expect(dataLabel('en', 'alertEffect', 'Unknown')).toBe('Unknown effect');
    expect(dataLabel('fi', 'alertScope', 'route')).toBe('Linja');
    expect(dataLabel('en', 'qualityCheck', 'is_stale_observation')).toBe('Stale observation');
    expect(dataLabel('fi', 'collector', 'siri_vm')).toBe('Ajoneuvoseuranta');
  });
});
