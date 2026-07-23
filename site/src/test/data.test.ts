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

describe('public data decoders', () => {
  it('accepts the expected top-level contracts', () => {
    expect(
      decodeOverview({
        schema_version: 1,
        meta: {},
        summary: {},
        takeaways: [],
        caveats: [{ id: 'snapshot', fi: 'Otos', en: 'Snapshot' }],
        hourly_profile: [{ local_hour: 8, bucket_count: 30, p90_delay_min: 2 }],
      }).schema_version,
    ).toBe(1);
    expect(
      decodeLines({
        schema_version: 1,
        lines: [{ line_ref: '3', line_name: '3', bucket_count: 30, p90_delay_min: 2 }],
        contexts: [
          {
            line_ref: '3',
            direction_ref: '1',
            day_type: 'weekday',
            local_hour: 8,
            p90_delay_min: 2,
          },
        ],
      }).lines,
    ).toHaveLength(1);
    expect(
      decodeStops({
        schema_version: 1,
        stops: [{ stop_id: '10', stop_name: 'Stop', line_count: 1 }],
        metrics: [
          { stop_id: '10', line_ref: 'all', day_type: 'all', bucket_count: 30, p90_delay_min: 2 },
        ],
      }).stops,
    ).toHaveLength(1);
    expect(
      decodeContext({
        schema_version: 1,
        rush_impact: [{ line_ref: '3', rush_p90_delay_lift_min: 1 }],
        alerts: [],
        quality: [{ quality_check: 'analysis_rows', row_count: 10, pct_rows: 100 }],
        collector_gaps: [],
        stop_changes: [],
      }).quality,
    ).toHaveLength(1);
  });

  it('rejects incompatible schemas and malformed rows', () => {
    expect(() => decodeOverview({ schema_version: 2 })).toThrow(/schema version/);
    expect(() => decodeLines({ schema_version: 1, lines: [{}], contexts: [] })).toThrow(/contract/);
    expect(() => decodeStops({ schema_version: 1, stops: 'not an array', metrics: [] })).toThrow(
      /array/,
    );
  });

  it('loads each bounded JSON resource through the configured base path', async () => {
    const payloads = [
      { schema_version: 1, meta: {}, summary: {}, takeaways: [], caveats: [], hourly_profile: [] },
      { schema_version: 1, lines: [], contexts: [] },
      { schema_version: 1, stops: [], metrics: [] },
      {
        schema_version: 1,
        rush_impact: [],
        alerts: [],
        quality: [],
        collector_gaps: [],
        stop_changes: [],
      },
    ];
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
