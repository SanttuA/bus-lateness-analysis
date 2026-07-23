import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MetricStrip } from '../components/MetricStrip';
import type { OverviewPayload } from '../types';

describe('MetricStrip', () => {
  it('renders localized snapshot metrics', () => {
    const overview = {
      meta: { conservative_excluded_pct: 5.69 },
      summary: {
        bucket_count: 3746770,
        line_count: 140,
        stop_count: 3566,
        p90_delay_min: 2.88,
        pct_over_5_min_late: 3.79,
      },
    } as OverviewPayload;
    render(<MetricStrip language="en" overview={overview} />);
    expect(screen.getByText('3,746,770')).toBeTruthy();
    expect(screen.getByText('network p90 delay')).toBeTruthy();
    expect(screen.getByText('5.69 %')).toBeTruthy();
  });
});
