import { describe, expect, it } from 'vitest';

import { dataLabel, t } from '../i18n';

describe('reader-facing labels', () => {
  it('uses concise explorer labels in both languages', () => {
    expect(t('fi').stopSearch).toBe('Pysäkin nimi tai tunnus');
    expect(t('en').stopSearch).toBe('Stop name or ID');
    expect(t('fi').alertPriority).toBe('Tiedoteprioriteetti');
    expect(t('fi').alertPriorityUnknown).toBe('tuntematon');
    expect(t('en').alertPriorityDescription).toContain('supplied by Föli');
    expect(t('en').alertPriorityDescription).toContain('shown as unknown');
  });

  it('translates public data codes instead of exposing raw identifiers', () => {
    expect(dataLabel('fi', 'alertCause', 'other_cause')).toBe('Muu syy');
    const finnishEffects = {
      NO_SERVICE: 'Ei liikennettä',
      REDUCED_SERVICE: 'Supistettu liikenne',
      SIGNIFICANT_DELAYS: 'Merkittäviä viiveitä',
      DETOUR: 'Poikkeusreitti',
      ADDITIONAL_SERVICE: 'Lisäliikenne',
      MODIFIED_SERVICE: 'Muutettu liikenne',
      OTHER_EFFECT: 'Muu vaikutus',
      UNKNOWN_EFFECT: 'Tuntematon vaikutus',
      STOP_MOVED: 'Pysäkki siirretty',
      Unknown: 'Tuntematon vaikutus',
    };
    for (const [effect, label] of Object.entries(finnishEffects)) {
      expect(dataLabel('fi', 'alertEffect', effect)).toBe(label);
    }
    expect(dataLabel('en', 'alertEffect', 'OTHER_EFFECT')).toBe('Other effect');
    expect(dataLabel('en', 'alertEffect', 'UNKNOWN_EFFECT')).toBe('Unknown effect');
    expect(dataLabel('fi', 'alertScope', 'route')).toBe('Linja');
    expect(dataLabel('en', 'qualityCheck', 'is_stale_observation')).toBe('Stale observation');
    expect(dataLabel('fi', 'collector', 'siri_vm')).toBe('Ajoneuvoseuranta');
  });
});
