import * as Plot from '@observablehq/plot';
import { useCallback } from 'react';

import { formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import type { Language, LineMetric } from '../types';
import { PlotFigure } from './PlotFigure';

interface RankingChartProps {
  language: Language;
  rows: LineMetric[];
  mode: 'late' | 'early';
}

export function RankingChart({ language, rows, mode }: RankingChartProps) {
  const copy = t(language);
  const metric = mode === 'late' ? 'p90_delay_min' : 'p90_early_min_abs';
  const color = mode === 'late' ? '#c86c24' : '#316f9e';
  const title = mode === 'late' ? copy.lateChart : copy.earlyChart;
  const subtitle = mode === 'late' ? copy.lateChartSub : copy.earlyChartSub;
  const top = [...rows]
    .sort((a, b) => b[metric] - a[metric] || b.bucket_count - a.bucket_count)
    .slice(0, 10);
  const render = useCallback(
    (width: number) =>
      Plot.plot({
        width,
        height: 430,
        marginTop: 14,
        marginRight: 76,
        marginBottom: 52,
        marginLeft: 70,
        style: {
          background: 'transparent',
          color: '#252c2e',
          fontFamily: 'inherit',
          fontSize: '16px',
        },
        x: {
          grid: true,
          label: language === 'fi' ? 'minuuttia' : 'minutes',
        },
        y: { label: null, domain: top.map((row) => row.line_name) },
        marks: [
          Plot.barX(top, {
            x: metric,
            y: 'line_name',
            fill: color,
            stroke: mode === 'late' ? '#8f4516' : '#1f4f73',
            tip: true,
          }),
          Plot.text(top, {
            x: metric,
            y: 'line_name',
            text: (row) => formatNumber(row[metric], language, 1),
            dx: 5,
            textAnchor: 'start',
            fill: '#252c2e',
            fontWeight: 700,
          }),
        ],
      }),
    [color, language, metric, mode, top],
  );
  return (
    <PlotFigure title={title} subtitle={subtitle} render={render}>
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.line}</th>
              <th scope="col">{mode === 'late' ? copy.p90 : copy.earlyP90}</th>
              <th scope="col">{mode === 'late' ? copy.overFive : copy.overThreeEarly}</th>
              <th scope="col">{copy.buckets}</th>
            </tr>
          </thead>
          <tbody>
            {top.map((row) => (
              <tr key={row.line_ref}>
                <th scope="row">{row.line_name}</th>
                <td>{formatMinutes(row[metric], language, 2)}</td>
                <td>
                  {formatPercent(
                    mode === 'late' ? row.pct_over_5_min_late : row.pct_over_3_min_early,
                    language,
                    2,
                  )}
                </td>
                <td>{formatNumber(row.bucket_count, language)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
