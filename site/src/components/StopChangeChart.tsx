import * as Plot from '@observablehq/plot';
import { useCallback } from 'react';

import { formatMinutes, formatNumber } from '../format';
import { t } from '../i18n';
import type { Language, StopChange } from '../types';
import { PlotFigure } from './PlotFigure';

export function StopChangeChart({ language, rows }: { language: Language; rows: StopChange[] }) {
  const copy = t(language);
  const top = [...rows]
    .sort((a, b) => Math.abs(b.p90_delay_change_min) - Math.abs(a.p90_delay_change_min))
    .slice(0, 10);
  const render = useCallback(
    (width: number) =>
      Plot.plot({
        width,
        height: 430,
        marginTop: 14,
        marginRight: 56,
        marginBottom: 54,
        marginLeft: width < 560 ? 150 : 176,
        style: {
          background: 'transparent',
          color: '#252c2e',
          fontFamily: 'inherit',
          fontSize: '16px',
        },
        x: { grid: true, label: language === 'fi' ? 'p90-muutos, min' : 'p90 change, min' },
        y: { label: null, domain: top.map((row) => row.stop_name) },
        color: { domain: ['improved', 'worsened'], range: ['#4f8fbd', '#d77a31'], legend: false },
        marks: [
          Plot.ruleX([0], { stroke: '#4d5655', strokeWidth: 1.5 }),
          Plot.barX(top, {
            x: 'p90_delay_change_min',
            y: 'stop_name',
            fill: (row) => (row.p90_delay_change_min < 0 ? 'improved' : 'worsened'),
            stroke: '#4d5655',
            tip: true,
          }),
        ],
      }),
    [language, top],
  );
  return (
    <PlotFigure
      title={
        language === 'fi'
          ? 'Suurimmat pysäkkikohtaiset p90-muutokset'
          : 'Largest stop-level p90 changes'
      }
      subtitle={
        language === 'fi'
          ? 'Jakson toinen puolisko verrattuna ensimmäiseen; sininen parani, oranssi heikkeni'
          : 'Second half versus first; blue improved, orange worsened'
      }
      render={render}
    >
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.stop}</th>
              <th scope="col">{language === 'fi' ? 'p90-muutos' : 'p90 change'}</th>
              <th scope="col">{language === 'fi' ? 'Luokkia yhteensä' : 'Total buckets'}</th>
            </tr>
          </thead>
          <tbody>
            {top.map((row) => (
              <tr key={row.stop_id}>
                <th scope="row">{row.stop_name}</th>
                <td>{formatMinutes(row.p90_delay_change_min, language, 2)}</td>
                <td>
                  {formatNumber(row.baseline_bucket_count + row.comparison_bucket_count, language)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
