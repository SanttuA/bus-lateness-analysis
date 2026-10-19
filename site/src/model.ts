import type {
  DayFilter,
  DayType,
  DelayDirection,
  LineHourMetric,
  LineMetric,
  LineScope,
  LinesPayload,
  StopMapPoint,
  StopsPayload,
} from './types';

export const PUNCTUALITY_BANDS = [
  'earlyStrong',
  'earlyMild',
  'onTime',
  'lateMild',
  'lateStrong',
] as const;
export type PunctualityBand = (typeof PUNCTUALITY_BANDS)[number];

export interface PunctualityShares {
  earlyStrong: number;
  earlyMild: number;
  onTime: number;
  lateMild: number;
  lateStrong: number;
}

type ShareSource = Pick<
  LineMetric,
  | 'pct_over_3_min_early'
  | 'pct_over_1_min_early'
  | 'pct_on_time'
  | 'pct_over_3_min_late'
  | 'pct_over_5_min_late'
>;

/** Split a line's stop passes into five punctuality bands that sum to 100 %. */
export function punctualityShares(row: ShareSource): PunctualityShares {
  const clamp = (value: number) => Math.max(0, value);
  return {
    earlyStrong: clamp(row.pct_over_3_min_early),
    earlyMild: clamp(row.pct_over_1_min_early - row.pct_over_3_min_early),
    onTime: clamp(row.pct_on_time),
    lateMild: clamp(row.pct_over_3_min_late - row.pct_over_5_min_late),
    lateStrong: clamp(row.pct_over_5_min_late),
  };
}

export interface PunctualitySegment {
  line_ref: string;
  line_name: string;
  band: PunctualityBand;
  share: number;
  x1: number;
  x2: number;
}

/**
 * Diverging segments around zero: early bands extend left, late bands right. The on-time
 * share is reported as a number instead of a bar so both problem sides align at zero.
 */
export function punctualitySegments(
  rows: (ShareSource & Pick<LineMetric, 'line_ref' | 'line_name'>)[],
) {
  const segments: PunctualitySegment[] = [];
  for (const row of rows) {
    const shares = punctualityShares(row);
    const base = { line_ref: row.line_ref, line_name: row.line_name };
    segments.push(
      { ...base, band: 'earlyMild', share: shares.earlyMild, x1: -shares.earlyMild, x2: 0 },
      {
        ...base,
        band: 'earlyStrong',
        share: shares.earlyStrong,
        x1: -(shares.earlyMild + shares.earlyStrong),
        x2: -shares.earlyMild,
      },
      { ...base, band: 'lateMild', share: shares.lateMild, x1: 0, x2: shares.lateMild },
      {
        ...base,
        band: 'lateStrong',
        share: shares.lateStrong,
        x1: shares.lateMild,
        x2: shares.lateMild + shares.lateStrong,
      },
    );
  }
  return segments;
}

/** Lines for the ranking: frequent, reliable lines by default; every line on request. */
export function selectRankedLines(
  lines: LineMetric[],
  scope: LineScope,
  mode: DelayDirection,
  frequentTripsPerDay: number,
  limit = 12,
): LineMetric[] {
  const eligible =
    scope === 'frequent'
      ? lines.filter((row) => !row.low_quality && row.trips_per_day >= frequentTripsPerDay)
      : lines;
  const key = mode === 'late' ? 'pct_over_5_min_late' : 'pct_over_3_min_early';
  return [...eligible]
    .sort(
      (a, b) =>
        b[key] - a[key] ||
        b.bucket_count - a.bucket_count ||
        a.line_name.localeCompare(b.line_name, undefined, { numeric: true }),
    )
    .slice(0, limit);
}

export function selectExplorableLines(data: LinesPayload): string[] {
  const linesWithProfiles = new Set(data.contexts.map((row) => row.line_ref));
  return data.lines
    .filter((row) => linesWithProfiles.has(row.line_ref))
    .sort((a, b) => a.line_name.localeCompare(b.line_name, undefined, { numeric: true }))
    .map((row) => row.line_ref);
}

/** The line with the most published hour cells (richest heatmap), then most buckets. */
export function selectDefaultLine(data: LinesPayload): string | undefined {
  const coverage = new Map<string, { cells: number; buckets: number }>();
  for (const row of data.contexts) {
    const current = coverage.get(row.line_ref) ?? { cells: 0, buckets: 0 };
    current.cells += 1;
    current.buckets += row.bucket_count;
    coverage.set(row.line_ref, current);
  }
  return [...coverage.entries()].sort(
    ([lineA, a], [lineB, b]) =>
      b.cells - a.cells ||
      b.buckets - a.buckets ||
      lineA.localeCompare(lineB, undefined, { numeric: true }),
  )[0]?.[0];
}

export interface HeatmapRow {
  key: string;
  direction_ref: string;
  day_type: DayType;
}

export interface HeatmapCell extends HeatmapRow {
  local_hour: number;
  value: number | null;
  context: LineHourMetric | null;
}

export function heatmapValue(row: LineHourMetric, mode: DelayDirection) {
  return mode === 'late' ? row.pct_over_5_min_late : row.pct_over_3_min_early;
}

/** Every hour × (direction, day type) cell for one line; missing groups stay null. */
export function selectHeatmapCells(
  data: LinesPayload,
  line: string,
  mode: DelayDirection,
): { rows: HeatmapRow[]; cells: HeatmapCell[] } {
  const contexts = data.contexts.filter((row) => row.line_ref === line);
  const directions = [...new Set(contexts.map((row) => row.direction_ref))].sort();
  const rows: HeatmapRow[] = [];
  for (const direction_ref of directions) {
    for (const day_type of ['weekday', 'weekend'] as const) {
      rows.push({ key: `${direction_ref}|${day_type}`, direction_ref, day_type });
    }
  }
  const byKey = new Map(
    contexts.map((row) => [`${row.direction_ref}|${row.day_type}|${row.local_hour}`, row]),
  );
  const cells: HeatmapCell[] = [];
  for (const row of rows) {
    for (let hour = 0; hour < 24; hour += 1) {
      const context = byKey.get(`${row.key}|${hour}`) ?? null;
      cells.push({
        ...row,
        local_hour: hour,
        value: context ? heatmapValue(context, mode) : null,
        context,
      });
    }
  }
  return { rows, cells };
}

/** Quantile thresholds splitting values into `bins` classes (for a binned map legend). */
export function quantileThresholds(values: number[], bins: number): number[] {
  const sorted = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!sorted.length || bins < 2) return [];
  const thresholds: number[] = [];
  for (let index = 1; index < bins; index += 1) {
    const position = (sorted.length - 1) * (index / bins);
    const lower = Math.floor(position);
    const upper = Math.ceil(position);
    const low = sorted[lower] ?? 0;
    const high = sorted[upper] ?? low;
    const value = low + (high - low) * (position - lower);
    thresholds.push(Math.round(value * 10) / 10);
  }
  return [...new Set(thresholds)];
}

export function binIndex(value: number, thresholds: number[]): number {
  let index = 0;
  while (index < thresholds.length && value >= (thresholds[index] ?? Infinity)) index += 1;
  return index;
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
        display_value:
          selection.mode === 'late' ? row.pct_over_5_min_late : row.pct_over_3_min_early,
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

/** Stop names repeat (one per platform); append the id where a name is not unique. */
export function disambiguatedStopNames(points: { stop_id: string; stop_name: string }[]) {
  const counts = new Map<string, number>();
  for (const point of points) counts.set(point.stop_name, (counts.get(point.stop_name) ?? 0) + 1);
  return new Map(
    points.map((point) => [
      point.stop_id,
      (counts.get(point.stop_name) ?? 0) > 1
        ? `${point.stop_name} (${point.stop_id})`
        : point.stop_name,
    ]),
  );
}
