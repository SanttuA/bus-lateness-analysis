import { describe, expect, it } from 'vitest';

import {
  selectDefaultLine,
  selectExplorableLines,
  selectLineContexts,
  selectStopPoints,
} from '../model';
import type { LinesPayload, StopsPayload } from '../types';

const metric = {
  bucket_count: 30,
  raw_poll_count: 60,
  signed_mean_delay_min: 1,
  median_delay_min: 0.5,
  p90_delay_min: 4,
  pct_over_5_min_late: 5,
  pct_over_3_min_early: 2,
  p90_early_min_abs: 3,
  pct_early: 40,
  pct_over_3_min_late: 8,
  pct_over_1_min_early: 20,
  p75_delay_min: 2,
  p95_delay_min: 5,
};

describe('explorer filtering', () => {
  it('excludes line options without an hourly profile', () => {
    const line = (line_ref: string) => ({ ...metric, line_ref, line_name: line_ref });
    const context = (line_ref: string) => ({
      ...line(line_ref),
      direction_ref: '1' as const,
      day_type: 'weekday' as const,
      local_hour: 8,
    });
    const data = {
      schema_version: 1,
      lines: [line('N14'), line('10'), line('2')],
      contexts: [context('10'), context('2')],
    } as LinesPayload;

    expect(selectExplorableLines(data)).toEqual(['2', '10']);
  });

  it('selects line contexts and orders hours', () => {
    const data = {
      schema_version: 1,
      lines: [],
      contexts: [
        {
          ...metric,
          line_ref: '3',
          line_name: '3',
          direction_ref: '1',
          day_type: 'weekday',
          local_hour: 9,
        },
        {
          ...metric,
          line_ref: '3',
          line_name: '3',
          direction_ref: '1',
          day_type: 'weekday',
          local_hour: 8,
        },
        {
          ...metric,
          line_ref: '4',
          line_name: '4',
          direction_ref: '1',
          day_type: 'weekday',
          local_hour: 8,
        },
      ],
    } as LinesPayload;
    expect(
      selectLineContexts(data, '3', '1', 'weekday', 8, 9).map((row) => row.local_hour),
    ).toEqual([8, 9]);
  });

  it('defaults to the line with the richest visible hourly profile', () => {
    const context = (line_ref: string, local_hour: number, bucket_count: number) => ({
      ...metric,
      line_ref,
      line_name: line_ref,
      direction_ref: '1' as const,
      day_type: 'weekday' as const,
      local_hour,
      bucket_count,
    });
    const data = {
      schema_version: 1,
      lines: [],
      contexts: [
        context('612', 6, 300),
        context('612', 7, 300),
        context('3', 6, 100),
        context('3', 7, 100),
        context('3', 8, 100),
      ],
    } as LinesPayload;

    expect(selectDefaultLine(data, '1', 'weekday')).toBe('3');
  });

  it('joins canonical stops, filters search, and switches metric direction', () => {
    const data = {
      schema_version: 1,
      stops: [
        { stop_id: '10', stop_name: 'Kauppatori', stop_lat: 60.45, stop_lon: 22.27, line_count: 8 },
        { stop_id: '20', stop_name: 'Satama', stop_lat: 60.44, stop_lon: 22.23, line_count: 2 },
      ],
      metrics: [
        { ...metric, stop_id: '10', line_ref: 'all', day_type: 'all' },
        {
          ...metric,
          p90_delay_min: 6,
          p90_early_min_abs: 1,
          stop_id: '20',
          line_ref: 'all',
          day_type: 'all',
        },
      ],
    } as StopsPayload;
    expect(
      selectStopPoints(data, {
        line: 'all',
        day: 'all',
        mode: 'late',
        query: 'sat',
        locale: 'fi-FI',
      })[0]?.stop_id,
    ).toBe('20');
    expect(
      selectStopPoints(data, {
        line: 'all',
        day: 'all',
        mode: 'early',
        query: '',
        locale: 'fi-FI',
      })[0]?.stop_id,
    ).toBe('10');
  });
});
