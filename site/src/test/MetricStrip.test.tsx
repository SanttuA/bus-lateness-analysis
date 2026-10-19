import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MetricStrip } from '../components/MetricStrip';
import type { OverviewPayload } from '../types';

describe('MetricStrip', () => {
  it('leads with reader-facing punctuality shares', () => {
    const overview = {
      summary: {
        pct_on_time: 65.41,
        pct_over_5_min_late: 3.8,
        pct_over_3_min_early: 7.83,
        p90_delay_min: 2.88,
        trip_count: 102868,
        day_count: 31,
      },
    } as OverviewPayload;
    render(<MetricStrip language="en" overview={overview} />);
    expect(screen.getByText('65.4 %')).toBeTruthy();
    expect(screen.getByText('of stop passes on time')).toBeTruthy();
    expect(screen.getByText('102,868')).toBeTruthy();
    expect(screen.getByText('2.9 min')).toBeTruthy();
  });
});
