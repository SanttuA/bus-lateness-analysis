import * as Plot from '@observablehq/plot';
import { useCallback } from 'react';

import { formatMinutes, formatNumber } from '../format';
import { t } from '../i18n';
import type { HourMetric, Language } from '../types';
import { PlotFigure } from './PlotFigure';

export function HourlyChart({ language, rows }: { language: Language; rows: HourMetric[] }) {
  const copy = t(language);
  const ordered = [...rows].sort((a, b) => a.local_hour - b.local_hour);
  const render = useCallback(
    (width: number) =>
      Plot.plot({
        width,
        height: width < 560 ? 380 : 460,
        marginTop: 20,
        marginRight: 30,
        marginBottom: 54,
        marginLeft: 68,
        style: {
          background: 'transparent',
          color: '#252c2e',
          fontFamily: 'inherit',
          fontSize: '16px',
        },
        x: {
          label: language === 'fi' ? 'paikallinen tunti' : 'local hour',
          ticks: width < 560 ? 6 : 12,
        },
        y: { label: language === 'fi' ? 'viive, min' : 'delay, min', grid: true },
        marks: [
          Plot.ruleY([0], { stroke: '#737b79' }),
          Plot.areaY(ordered, {
            x: 'local_hour',
            y: 'p90_delay_min',
            fill: '#d4873f',
            fillOpacity: 0.14,
          }),
          Plot.lineY(ordered, {
            x: 'local_hour',
            y: 'p90_delay_min',
            stroke: '#b85b1b',
            strokeWidth: 3,
            tip: true,
          }),
          Plot.dot(ordered, { x: 'local_hour', y: 'p90_delay_min', fill: '#b85b1b', r: 3 }),
          Plot.lineY(ordered, {
            x: 'local_hour',
            y: 'median_delay_min',
            stroke: '#316f9e',
            strokeWidth: 2,
            strokeDasharray: '5,4',
            tip: true,
          }),
        ],
      }),
    [language, ordered],
  );
  return (
    <PlotFigure title={copy.hourlyChart} subtitle={copy.hourlyChartSub} render={render}>
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{language === 'fi' ? 'Tunti' : 'Hour'}</th>
              <th scope="col">{copy.median}</th>
              <th scope="col">{copy.p90}</th>
              <th scope="col">{copy.buckets}</th>
            </tr>
          </thead>
          <tbody>
            {ordered.map((row) => (
              <tr key={row.local_hour}>
                <th scope="row">{String(row.local_hour).padStart(2, '0')}:00</th>
                <td>{formatMinutes(row.median_delay_min, language, 2)}</td>
                <td>{formatMinutes(row.p90_delay_min, language, 2)}</td>
                <td>{formatNumber(row.bucket_count, language)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
