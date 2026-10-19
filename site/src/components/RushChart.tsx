import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatInterval, formatNumber, formatPercent, formatSigned } from '../format';
import { t } from '../i18n';
import { textWidth } from '../measure';
import { plotStyle, theme } from '../theme';
import type { Language, RushImpact } from '../types';
import { ChartLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';

const OFFPEAK = theme.punctuality.lateMild;
const RUSH = theme.punctuality.lateStrong;

export function RushChart({ language, rows }: { language: Language; rows: RushImpact[] }) {
  const copy = t(language);
  const top = useMemo(
    () =>
      [...rows]
        .sort(
          (a, b) => b.rush_over_5_min_late_pct_point_lift - a.rush_over_5_min_late_pct_point_lift,
        )
        .slice(0, 10),
    [rows],
  );
  const pp = language === 'fi' ? '%-yks.' : 'pp';
  const render = useCallback(
    (width: number) => {
      const liftText = (row: RushImpact, unit: boolean) =>
        `${formatSigned(row.rush_over_5_min_late_pct_point_lift, language, 1)}${unit ? ` ${pp}` : ''}`;
      // Lift labels sit right of each line's furthest dot; drop the unit (the table keeps it)
      // when the full label would take too much of a narrow chart.
      const withUnit =
        textWidth(
          top.map((row) => liftText(row, true)),
          { fontWeight: 600 },
        ) + 18;
      const unit = withUnit <= width * 0.3;
      const marginRight = unit
        ? withUnit
        : textWidth(
            top.map((row) => liftText(row, false)),
            { fontWeight: 600 },
          ) + 18;
      const maxValue = Math.max(
        10,
        ...top.map((row) =>
          Math.max(row.pct_over_5_min_late_rush, row.pct_over_5_min_late_offpeak),
        ),
      );
      return Plot.plot({
        width,
        height: 60 + top.length * 34,
        marginTop: 14,
        marginRight,
        marginBottom: 44,
        marginLeft: Math.max(40, textWidth(top.map((row) => row.line_name)) + 14),
        style: plotStyle,
        x: {
          domain: [0, Math.ceil(maxValue / 10) * 10],
          grid: true,
          label: `${copy.overFive}, %`,
          tickFormat: (value: number) => formatNumber(value, language, 0),
        },
        y: { domain: top.map((row) => row.line_name), label: null, tickSize: 0 },
        marks: [
          Plot.link(top, {
            x1: 'pct_over_5_min_late_offpeak',
            x2: 'pct_over_5_min_late_rush',
            y: 'line_name',
            stroke: theme.baseline,
            strokeWidth: 2,
          }),
          Plot.dot(top, {
            x: 'pct_over_5_min_late_offpeak',
            y: 'line_name',
            r: 5,
            fill: theme.surface,
            stroke: OFFPEAK,
            strokeWidth: 2.5,
          }),
          Plot.dot(top, {
            x: 'pct_over_5_min_late_rush',
            y: 'line_name',
            r: 5,
            fill: RUSH,
            stroke: theme.surface,
            strokeWidth: 2,
            channels: {
              offpeak: {
                value: (row: RushImpact) =>
                  formatPercent(row.pct_over_5_min_late_offpeak, language, 1),
                label: copy.offpeak,
              },
              rush: {
                value: (row: RushImpact) =>
                  formatPercent(row.pct_over_5_min_late_rush, language, 1),
                label: copy.rush,
              },
              trips: {
                value: (row: RushImpact) => `${row.trip_count_offpeak} / ${row.trip_count_rush}`,
                label: copy.trips,
              },
            },
            tip: {
              format: { x: false, fill: false, y: true, offpeak: true, rush: true, trips: true },
            },
          }),
          Plot.text(top, {
            x: (row: RushImpact) =>
              Math.max(row.pct_over_5_min_late_rush, row.pct_over_5_min_late_offpeak),
            y: 'line_name',
            text: (row: RushImpact) => liftText(row, unit),
            dx: 12,
            textAnchor: 'start',
            fill: theme.ink,
            fontWeight: 600,
          }),
        ],
      });
    },
    [copy, language, pp, top],
  );
  return (
    <PlotFigure
      title={copy.rushChart}
      subtitle={copy.rushChartSub}
      render={render}
      legend={
        <ChartLegend
          label={copy.rushChart}
          items={[
            { label: copy.offpeak, color: OFFPEAK, shape: 'ring' },
            { label: copy.rush, color: RUSH, shape: 'dot' },
          ]}
        />
      }
    >
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.line}</th>
              <th scope="col">{copy.offpeak}</th>
              <th scope="col">{copy.rush}</th>
              <th scope="col">{copy.lift}</th>
              <th scope="col">{copy.trips}</th>
            </tr>
          </thead>
          <tbody>
            {top.map((row) => (
              <tr key={row.line_ref}>
                <th scope="row">{row.line_name}</th>
                <td>{formatPercent(row.pct_over_5_min_late_offpeak, language, 1)}</td>
                <td>{formatPercent(row.pct_over_5_min_late_rush, language, 1)}</td>
                <td>
                  {formatSigned(row.rush_over_5_min_late_pct_point_lift, language, 1)} {pp}
                  {formatInterval(
                    row.rush_over_5_min_late_lift_ci_lo,
                    row.rush_over_5_min_late_lift_ci_hi,
                    language,
                    'pp',
                  )}
                </td>
                <td>
                  {formatNumber(row.trip_count_offpeak, language)} /{' '}
                  {formatNumber(row.trip_count_rush, language)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
