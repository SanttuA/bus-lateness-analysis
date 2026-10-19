import { describe, expect, it, vi } from 'vitest';

import {
  decodeContext,
  decodeLines,
  decodeOverview,
  decodeStops,
  loadContext,
  loadLines,
  loadOverview,
  loadStops,
} from '../data';

const overview = {
  schema_version: 2,
  meta: {},
  summary: {},
  headlines: {
    fi: { ranking: 'a', recommendations: [] },
    en: { ranking: 'a', recommendations: [] },
  },
  takeaways: [],
  caveats: [{ id: 'snapshot', fi: 'Otos', en: 'Snapshot' }],
  hourly_profile: [
    { day_type: 'weekday', local_hour: 8, bucket_count: 30, pct_over_5_min_late: 2 },
  ],
};
const lines = {
  schema_version: 2,
  lines: [
    {
      line_ref: '3',
      line_name: '3',
      trip_count: 30,
      pct_on_time: 60,
      pct_over_5_min_late: 2,
      low_quality: false,
    },
  ],
  contexts: [
    {
      line_ref: '3',
      direction_ref: '1',
      day_type: 'weekday',
      local_hour: 8,
      pct_over_5_min_late: 2,
    },
  ],
};
const stops = {
  schema_version: 2,
  stops: [{ stop_id: '10', stop_name: 'Stop', line_count: 1 }],
  metrics: [
    { stop_id: '10', line_ref: 'all', day_type: 'all', bucket_count: 30, pct_over_5_min_late: 2 },
  ],
};
const context = {
  schema_version: 2,
  rush_impact: [{ line_ref: '3', rush_over_5_min_late_pct_point_lift: 1 }],
  alerts: [{ cause: 'OTHER_CAUSE', effect: 'DETOUR', late5_lift_pp: 1 }],
  alert_inventory: { included: 1, long_running: 0, no_targets: 0, never_active: 0 },
  cancellations: { total_departures: 0, by_line: [], by_cause: [] },
  quality: [{ quality_check: 'analysis_rows', row_count: 10, pct_rows: 100 }],
  collector_gaps: [],
  daily: [{ local_date: '2026-05-01', day_type: 'weekend', pct_on_time: 64 }],
};

describe('public data decoders', () => {
  it('accepts the schema version 2 contracts', () => {
    expect(decodeOverview(overview).schema_version).toBe(2);
    expect(decodeLines(lines).lines).toHaveLength(1);
    expect(decodeStops(stops).stops).toHaveLength(1);
    expect(decodeContext(context).daily).toHaveLength(1);
  });

  it('rejects older schemas, missing headlines and malformed rows', () => {
    expect(() => decodeOverview({ ...overview, schema_version: 1 })).toThrow(/schema version/);
    expect(() => decodeOverview({ ...overview, headlines: { fi: {} } })).toThrow(/headlines/);
    expect(() => decodeLines({ schema_version: 2, lines: [{}], contexts: [] })).toThrow(/contract/);
    expect(() => decodeStops({ schema_version: 2, stops: 'not an array', metrics: [] })).toThrow(
      /array/,
    );
    expect(() => decodeContext({ ...context, alert_inventory: undefined })).toThrow(
      /alert_inventory/,
    );
  });

  it('loads each bounded JSON resource through the configured base path', async () => {
    const payloads = [overview, lines, stops, context];
    const fetchMock = vi
      .fn<() => Promise<{ ok: boolean; json: () => Promise<unknown> }>>()
      .mockImplementation(async () => ({
        ok: true,
        json: async () => payloads.shift(),
      }));
    vi.stubGlobal('fetch', fetchMock);

    await loadOverview();
    await loadLines();
    await loadStops();
    await loadContext();

    expect(fetchMock).toHaveBeenCalledTimes(4);
    vi.unstubAllGlobals();
  });

  it('reports HTTP failures', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 503 }));
    await expect(loadOverview()).rejects.toThrow(/503/);
    vi.unstubAllGlobals();
  });
});
