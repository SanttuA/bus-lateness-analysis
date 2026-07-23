import * as Plot from '@observablehq/plot';
import { useCallback } from 'react';

import { formatMinutes, formatNumber } from '../format';
import { t } from '../i18n';
import type { Language, RushImpact } from '../types';
import { PlotFigure } from './PlotFigure';

export function RushChart({ language, rows }: { language: Language; rows: RushImpact[] }) {
  const copy = t(language);
  const top = [...rows]
    .sort((a, b) => b.rush_p90_delay_lift_min - a.rush_p90_delay_lift_min)
    .slice(0, 8);
  const render = useCallback(
    (width: number) =>
      Plot.plot({
        width,
        height: 390,
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
        x: { grid: true, label: language === 'fi' ? 'p90-lisä, min' : 'p90 lift, min' },
        y: { label: null, domain: top.map((row) => row.line_name) },
        marks: [
          Plot.ruleX([0], { stroke: '#737b79' }),
          Plot.barX(top, {
            x: 'rush_p90_delay_lift_min',
            y: 'line_name',
            fill: '#c86c24',
            stroke: '#8f4516',
            tip: true,
          }),
          Plot.text(top, {
            x: 'rush_p90_delay_lift_min',
            y: 'line_name',
            text: (row) => formatNumber(row.rush_p90_delay_lift_min, language, 1),
            dx: 5,
            textAnchor: 'start',
            fontWeight: 700,
          }),
        ],
      }),
    [language, top],
  );
  return (
    <PlotFigure title={copy.rushChart} subtitle={copy.rushChartSub} render={render}>
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.line}</th>
              <th scope="col">{copy.p90Lift}</th>
              <th scope="col">{language === 'fi' ? 'Ruuhkaluokkia' : 'Rush buckets'}</th>
            </tr>
          </thead>
          <tbody>
            {top.map((row) => (
              <tr key={row.line_ref}>
                <th scope="row">{row.line_name}</th>
                <td>{formatMinutes(row.rush_p90_delay_lift_min, language, 2)}</td>
                <td>{formatNumber(row.bucket_count_rush, language)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
