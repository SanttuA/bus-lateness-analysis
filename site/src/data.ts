import type { ContextPayload, LinesPayload, OverviewPayload, StopsPayload } from './types';

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function assertPayload(
  value: unknown,
  arrays: string[],
  name: string,
): asserts value is JsonObject {
  if (!isObject(value) || value.schema_version !== 1) {
    throw new Error(`${name}: unsupported or missing schema version`);
  }
  for (const key of arrays) {
    if (!Array.isArray(value[key])) throw new Error(`${name}: ${key} must be an array`);
  }
}

function assertRows(rows: unknown[], required: string[], name: string) {
  for (const [index, row] of rows.entries()) {
    if (!isObject(row) || required.some((key) => !(key in row))) {
      throw new Error(`${name}: row ${index} does not match the public data contract`);
    }
  }
}

export function decodeOverview(value: unknown): OverviewPayload {
  assertPayload(value, ['takeaways', 'caveats', 'hourly_profile'], 'overview');
  if (!isObject(value.meta) || !isObject(value.summary)) {
    throw new Error('overview: meta and summary are required');
  }
  assertRows(
    value.hourly_profile as unknown[],
    ['local_hour', 'bucket_count', 'p90_delay_min'],
    'overview',
  );
  assertRows(value.caveats as unknown[], ['id', 'fi', 'en'], 'overview');
  return value as unknown as OverviewPayload;
}

export function decodeLines(value: unknown): LinesPayload {
  assertPayload(value, ['lines', 'contexts'], 'lines');
  assertRows(
    value.lines as unknown[],
    ['line_ref', 'line_name', 'bucket_count', 'p90_delay_min'],
    'lines',
  );
  assertRows(
    value.contexts as unknown[],
    ['line_ref', 'direction_ref', 'day_type', 'local_hour', 'p90_delay_min'],
    'lines',
  );
  return value as unknown as LinesPayload;
}

export function decodeStops(value: unknown): StopsPayload {
  assertPayload(value, ['stops', 'metrics'], 'stops');
  assertRows(value.stops as unknown[], ['stop_id', 'stop_name', 'line_count'], 'stops');
  assertRows(
    value.metrics as unknown[],
    ['stop_id', 'line_ref', 'day_type', 'bucket_count', 'p90_delay_min'],
    'stops',
  );
  return value as unknown as StopsPayload;
}

export function decodeContext(value: unknown): ContextPayload {
  assertPayload(
    value,
    ['rush_impact', 'alerts', 'quality', 'collector_gaps', 'stop_changes'],
    'context',
  );
  assertRows(value.rush_impact as unknown[], ['line_ref', 'rush_p90_delay_lift_min'], 'context');
  assertRows(value.quality as unknown[], ['quality_check', 'row_count', 'pct_rows'], 'context');
  return value as unknown as ContextPayload;
}

async function loadJson<T>(filename: string, decoder: (value: unknown) => T): Promise<T> {
  const response = await fetch(`${import.meta.env.BASE_URL}data/${filename}.json`);
  if (!response.ok) throw new Error(`Unable to load ${filename}.json (${response.status})`);
  return decoder(await response.json());
}

export const loadOverview = () => loadJson('overview', decodeOverview);
export const loadLines = () => loadJson('lines', decodeLines);
export const loadStops = () => loadJson('stops', decodeStops);
export const loadContext = () => loadJson('context', decodeContext);
