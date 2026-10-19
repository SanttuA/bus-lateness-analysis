import { describe, expect, it } from 'vitest';

import { labelStep, textWidth, wrapText } from '../measure';

// jsdom has no canvas text metrics, so these exercise the 0.6 em per character estimate.
describe('chart text measurement', () => {
  it('measures the widest text and the widest line of multi-line text', () => {
    expect(textWidth(['abcd'])).toBe(39);
    expect(textWidth(['ab', 'abcdefghij'])).toBe(96);
    expect(textWidth(['Suunta 1\nArki'])).toBe(textWidth(['Suunta 1']));
    expect(textWidth([])).toBe(0);
    expect(textWidth(['abcd'], { fontSize: 10 })).toBe(24);
  });

  it('wraps greedily without splitting words', () => {
    // Each character is ~9.6px at 16px, so 'prior. 1000' (106px) fits in 110px.
    expect(wrapText('Poikkeusreitti · linja · prior. 1000', 110)).toEqual([
      'Poikkeusreitti',
      '· linja ·',
      'prior. 1000',
    ]);
    expect(wrapText('short', 100)).toEqual(['short']);
    expect(wrapText('Pysäkki siirretty · pysäkki', 1000)).toEqual(['Pysäkki siirretty · pysäkki']);
  });

  it('picks the smallest label step whose spacing fits the label', () => {
    expect(labelStep(30, 18)).toBe(1);
    expect(labelStep(10, 18)).toBe(3);
    expect(labelStep(1, 18)).toBe(12);
    expect(labelStep(40, 70, [1, 2, 3, 4], 12)).toBe(3);
  });
});
