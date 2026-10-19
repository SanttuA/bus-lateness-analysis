import { describe, expect, it } from 'vitest';

import {
  binIndex,
  disambiguatedStopNames,
  punctualitySegments,
  punctualityShares,
  quantileThresholds,
  selectDefaultLine,
  selectExplorableLines,
  selectHeatmapCells,
  selectRankedLines,
  selectStopPoints,
} from '../model';
import type { LineHourMetric, LineMetric, LinesPayload, StopsPayload } from '../types';

const shares = {
  bucket_count: 100,
  median_delay_min: 0.5,
  p90_delay_min: 4,
  pct_on_time: 60,
  pct_over_5_min_late: 5,
  pct_over_3_min_late: 15,
  pct_over_1_min_early: 25,
  pct_over_3_min_early: 10,
};

function line(line_ref: string, overrides: Partial<LineMetric> = {}): LineMetric {
  return {
    ...shares,
    line_ref,
    line_name: line_ref,
    trip_count: 500,
    day_count: 20,
    trips_per_day: 25,
    p90_delay_ci_lo: null,
    p90_delay_ci_hi: null,
    pct_over_5_min_late_ci_lo: null,
    pct_over_5_min_late_ci_hi: null,
    pct_over_3_min_early_ci_lo: null,
    pct_over_3_min_early_ci_hi: null,
    excluded_pct: 5,
    data_fault_pct: 1,
    trip_coverage_pct: 99,
    low_quality: false,
    direction_labels: {},
    ...overrides,
  };
}

function context(
  line_ref: string,
  local_hour: number,
  overrides: Partial<LineHourMetric> = {},
): LineHourMetric {
  return {
    ...shares,
    line_ref,
    direction_ref: '1',
    day_type: 'weekday',
    local_hour,
    trip_count: 20,
    ...overrides,
  };
}

describe('punctuality bands', () => {
  it('splits stop passes into five bands that sum to 100 %', () => {
    const bands = punctualityShares(shares);
    expect(bands).toEqual({
      earlyStrong: 10,
      earlyMild: 15,
      onTime: 60,
      lateMild: 10,
      lateStrong: 5,
    });
    expect(Object.values(bands).reduce((sum, value) => sum + value, 0)).toBe(100);
  });

  it('places early bands left of zero and late bands right of zero', () => {
    const segments = punctualitySegments([line('3')]);
    const byBand = Object.fromEntries(segments.map((segment) => [segment.band, segment]));
    expect(byBand.earlyMild).toMatchObject({ x1: -15, x2: 0 });
    expect(byBand.earlyStrong).toMatchObject({ x1: -25, x2: -15 });
    expect(byBand.lateMild).toMatchObject({ x1: 0, x2: 10 });
    expect(byBand.lateStrong).toMatchObject({ x1: 10, x2: 15 });
  });
});

describe('line ranking', () => {
  const lines = [
    line('24', { pct_over_5_min_late: 18 }),
    line('612', { pct_over_5_min_late: 46, trips_per_day: 2 }),
    line('K6', { pct_over_5_min_late: 30, low_quality: true }),
    line('P6', { pct_over_3_min_early: 60 }),
  ];

  it('defaults to frequent lines with complete data', () => {
    expect(selectRankedLines(lines, 'frequent', 'late', 20).map((row) => row.line_ref)).toEqual([
      '24',
      'P6',
    ]);
  });

  it('includes every line on request and sorts by the chosen side', () => {
    expect(selectRankedLines(lines, 'all', 'late', 20)[0]?.line_ref).toBe('612');
    expect(selectRankedLines(lines, 'all', 'early', 20)[0]?.line_ref).toBe('P6');
  });
});

describe('line explorer', () => {
  const data: LinesPayload = {
    schema_version: 2,
    lines: [line('N14'), line('10'), line('2')],
    contexts: [
      context('10', 8),
      context('10', 9, { pct_over_5_min_late: 12 }),
      context('10', 9, { direction_ref: '2', day_type: 'weekend' }),
      context('2', 8),
    ],
  };

  it('excludes line options without published hours', () => {
    expect(selectExplorableLines(data)).toEqual(['2', '10']);
  });

  it('defaults to the line with the richest heatmap', () => {
    expect(selectDefaultLine(data)).toBe('10');
  });

  it('builds 24 cells per direction and day type with gaps left empty', () => {
    const { rows, cells } = selectHeatmapCells(data, '10', 'late');
    expect(rows.map((row) => row.key)).toEqual([
      '1|weekday',
      '1|weekend',
      '2|weekday',
      '2|weekend',
    ]);
    expect(cells).toHaveLength(4 * 24);
    const cell = (key: string, hour: number) =>
      cells.find((item) => item.key === key && item.local_hour === hour);
    expect(cell('1|weekday', 9)?.value).toBe(12);
    expect(cell('1|weekday', 10)?.value).toBeNull();
    expect(selectHeatmapCells(data, '10', 'early').cells.find((item) => item.context)?.value).toBe(
      10,
    );
  });
});

describe('map classes', () => {
  it('splits values into equally sized classes', () => {
    const thresholds = quantileThresholds([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5);
    expect(thresholds).toEqual([2, 4, 6, 8]);
    expect(binIndex(0, thresholds)).toBe(0);
    expect(binIndex(4, thresholds)).toBe(2);
    expect(binIndex(10, thresholds)).toBe(4);
    expect(quantileThresholds([], 5)).toEqual([]);
  });

  it('joins canonical stops, filters search, and switches the displayed share', () => {
    const data: StopsPayload = {
      schema_version: 2,
      stops: [
        { stop_id: '10', stop_name: 'Kauppatori', stop_lat: 60.45, stop_lon: 22.27, line_count: 8 },
        { stop_id: '20', stop_name: 'Satama', stop_lat: 60.44, stop_lon: 22.23, line_count: 2 },
      ],
      metrics: [
        { ...shares, stop_id: '10', line_ref: 'all', day_type: 'all', pct_over_3_min_early: 30 },
        { ...shares, stop_id: '20', line_ref: 'all', day_type: 'all', pct_over_5_min_late: 9 },
      ],
    };
    const select = (mode: 'late' | 'early', query = '') =>
      selectStopPoints(data, { line: 'all', day: 'all', mode, query, locale: 'fi-FI' });
    expect(select('late')[0]?.stop_id).toBe('20');
    expect(select('late')[0]?.display_value).toBe(9);
    expect(select('early')[0]?.stop_id).toBe('10');
    expect(select('late', 'kauppa').map((point) => point.stop_id)).toEqual(['10']);
  });

  it('adds the stop id only where a name repeats', () => {
    const names = disambiguatedStopNames([
      { stop_id: '1804', stop_name: 'Prisma' },
      { stop_id: '1805', stop_name: 'Prisma' },
      { stop_id: '10', stop_name: 'Kauppatori' },
    ]);
    expect(names.get('1804')).toBe('Prisma (1804)');
    expect(names.get('10')).toBe('Kauppatori');
  });
});
