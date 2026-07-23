import type { SearchState } from './types';

function stringValue(value: unknown): string | undefined {
  if (typeof value === 'string') return value.trim() || undefined;
  return typeof value === 'number' && Number.isFinite(value) ? String(value) : undefined;
}

function hourValue(value: unknown): number | undefined {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isInteger(parsed) && parsed >= 0 && parsed <= 23 ? parsed : undefined;
}

export function validateSearch(search: Record<string, unknown>): SearchState {
  const direction = stringValue(search.direction);
  const day = stringValue(search.day);
  const metric = stringValue(search.metric);
  const view = stringValue(search.view);
  const from = hourValue(search.from);
  const to = hourValue(search.to);
  return {
    ...(stringValue(search.line) ? { line: stringValue(search.line) } : {}),
    ...(direction === '1' || direction === '2' ? { direction } : {}),
    ...(day === 'weekday' || day === 'weekend' ? { day } : {}),
    ...(from !== undefined ? { from } : {}),
    ...(to !== undefined ? { to } : {}),
    ...(stringValue(search.stop) ? { stop: stringValue(search.stop) } : {}),
    ...(metric === 'late' || metric === 'early' ? { metric } : {}),
    ...(view === 'map' || view === 'table' ? { view } : {}),
  };
}

export function normalizeHourRange(from: number | undefined, to: number | undefined) {
  const start = from ?? 0;
  const end = to ?? 23;
  return start <= end ? { start, end } : { start: end, end: start };
}
