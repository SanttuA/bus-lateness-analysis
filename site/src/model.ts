import type {
  DayFilter,
  DelayDirection,
  LineHourMetric,
  LinesPayload,
  StopMapPoint,
  StopsPayload,
} from './types';

export function selectExplorableLines(data: LinesPayload): string[] {
  const linesWithProfiles = new Set(data.contexts.map((row) => row.line_ref));
  return data.lines
    .filter((row) => linesWithProfiles.has(row.line_ref))
    .sort((a, b) => a.line_name.localeCompare(b.line_name, undefined, { numeric: true }))
    .map((row) => row.line_ref);
}

export function selectLineContexts(
  data: LinesPayload,
  line: string,
  direction: '1' | '2',
  day: 'weekday' | 'weekend',
  start: number,
  end: number,
): LineHourMetric[] {
  return data.contexts
    .filter(
      (row) =>
        row.line_ref === line &&
        row.direction_ref === direction &&
        row.day_type === day &&
        row.local_hour >= start &&
        row.local_hour <= end,
    )
    .sort((a, b) => a.local_hour - b.local_hour);
}

export function selectDefaultLine(
  data: LinesPayload,
  direction: '1' | '2',
  day: 'weekday' | 'weekend',
): string | undefined {
  const coverage = new Map<string, { hours: Set<number>; buckets: number }>();
  for (const row of data.contexts) {
    if (row.direction_ref !== direction || row.day_type !== day) continue;
    const current = coverage.get(row.line_ref) ?? { hours: new Set<number>(), buckets: 0 };
    current.hours.add(row.local_hour);
    current.buckets += row.bucket_count;
    coverage.set(row.line_ref, current);
  }

  return [...coverage.entries()].sort(
    ([lineA, a], [lineB, b]) =>
      b.hours.size - a.hours.size ||
      b.buckets - a.buckets ||
      lineA.localeCompare(lineB, undefined, { numeric: true }),
  )[0]?.[0];
}

export interface StopSelection {
  line: string;
  day: DayFilter;
  mode: DelayDirection;
  query: string;
  locale: string;
}

export function selectStopPoints(data: StopsPayload, selection: StopSelection): StopMapPoint[] {
  const stopById = new Map(data.stops.map((stop) => [stop.stop_id, stop]));
  const query = selection.query.toLocaleLowerCase(selection.locale);
  return data.metrics
    .filter((row) => row.line_ref === selection.line && row.day_type === selection.day)
    .map((row): StopMapPoint | null => {
      const stop = stopById.get(row.stop_id);
      if (!stop) return null;
      return {
        ...stop,
        ...row,
        display_value: selection.mode === 'late' ? row.p90_delay_min : row.p90_early_min_abs,
      };
    })
    .filter((row): row is StopMapPoint => row !== null)
    .filter(
      (row) =>
        !query ||
        row.stop_id.toLocaleLowerCase(selection.locale).includes(query) ||
        row.stop_name.toLocaleLowerCase(selection.locale).includes(query),
    )
    .sort((a, b) => b.display_value - a.display_value || b.bucket_count - a.bucket_count);
}
