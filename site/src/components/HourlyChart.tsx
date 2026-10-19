import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { textWidth } from '../measure';
import { plotStyle, theme } from '../theme';
import type { DayType, HourMetric, Language } from '../types';
import { ChartLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';

const SERIES_COLOR: Record<DayType, string> = { weekday: theme.late, weekend: theme.deemphasis };

export function HourlyChart({ language, rows }: { language: Language; rows: HourMetric[] }) {
  const copy = t(language);
  const dayLabel = useCallback(
    (day: DayType) => (day === 'weekday' ? copy.weekday : copy.weekend),
    [copy.weekday, copy.weekend],
  );
  const ordered = useMemo(
    () =>
      [...rows].sort((a, b) => a.day_type.localeCompare(b.day_type) || a.local_hour - b.local_hour),
    [rows],
  );
  const lastPoints = useMemo(
    () =>
      (['weekday', 'weekend'] as const)
        .map((day) => ordered.filter((row) => row.day_type === day).at(-1))
        .filter((row): row is HourMetric => row !== undefined),
    [ordered],
  );
  const render = useCallback(
    (width: number) => {
      const narrow = width < 560;
      // Direct line labels need a right margin as wide as the longest label; drop them (the
      // legend names the series) when that would squeeze the chart.
      const directWidth = textWidth(lastPoints.map((row) => dayLabel(row.day_type))) + 16;
      const directLabels = !narrow && directWidth <= width * 0.3;
      const yTickWidth = textWidth(['100']);
      return Plot.plot({
        width,
        height: narrow ? 340 : 400,
        marginTop: 20,
        marginRight: directLabels ? directWidth : 24,
        marginBottom: 48,
        marginLeft: Math.max(52, yTickWidth + 22),
        style: plotStyle,
        x: {
          label: copy.hour,
          domain: [0, 23],
          ticks: narrow ? 6 : 12,
          tickFormat: (h: number) => `${h}`,
        },
        y: {
          label: `${copy.overFive}, %`,
          grid: true,
          zero: true,
          tickFormat: (value: number) => formatNumber(value, language, 0),
        },
        marks: [
          Plot.ruleY([0], { stroke: theme.baseline }),
          Plot.lineY(ordered, {
            x: 'local_hour',
            y: 'pct_over_5_min_late',
            z: 'day_type',
            stroke: (row: HourMetric) => SERIES_COLOR[row.day_type],
            strokeWidth: 2,
            curve: 'monotone-x',
          }),
          Plot.dot(ordered, {
            x: 'local_hour',
            y: 'pct_over_5_min_late',
            fill: (row: HourMetric) => SERIES_COLOR[row.day_type],
            stroke: theme.surface,
            strokeWidth: 2,
            r: 4,
          }),
          ...(!directLabels
            ? []
            : [
                Plot.text(lastPoints, {
                  x: 'local_hour',
                  y: 'pct_over_5_min_late',
                  text: (row: HourMetric) => dayLabel(row.day_type),
                  dx: 10,
                  textAnchor: 'start',
                  fill: theme.inkSoft,
                }),
              ]),
          Plot.ruleX(
            ordered,
            Plot.pointerX({ x: 'local_hour', stroke: theme.muted, strokeWidth: 1, maxRadius: 40 }),
          ),
          Plot.tip(
            ordered,
            Plot.pointerX({
              x: 'local_hour',
              y: 'pct_over_5_min_late',
              maxRadius: 40,
              title: (row: HourMetric) => {
                const other = ordered.find(
                  (candidate) =>
                    candidate.local_hour === row.local_hour && candidate.day_type !== row.day_type,
                );
                return [row, other]
                  .filter((item): item is HourMetric => item !== undefined)
                  .map(
                    (item) =>
                      `${formatPercent(item.pct_over_5_min_late, language, 1)} · ${dayLabel(item.day_type)}`,
                  )
                  .join('\n')
                  .concat(`\n${String(row.local_hour).padStart(2, '0')}:00`);
              },
            }),
          ),
        ],
      });
    },
    [copy, dayLabel, language, lastPoints, ordered],
  );
  return (
    <PlotFigure
      title={copy.hourlyChart}
      subtitle={copy.hourlyChartSub}
      render={render}
      legend={
        <ChartLegend
          label={copy.hourlyChart}
          items={(['weekday', 'weekend'] as const).map((day) => ({
            label: dayLabel(day),
            color: SERIES_COLOR[day],
            shape: 'line',
          }))}
        />
      }
    >
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.dayType}</th>
              <th scope="col">{copy.hour}</th>
              <th scope="col">{copy.onTime}</th>
              <th scope="col">{copy.overFive}</th>
              <th scope="col">{copy.median}</th>
              <th scope="col">{copy.p90}</th>
              <th scope="col">{copy.trips}</th>
            </tr>
          </thead>
          <tbody>
            {ordered.map((row) => (
              <tr key={`${row.day_type}-${row.local_hour}`}>
                <td>{dayLabel(row.day_type)}</td>
                <th scope="row">{String(row.local_hour).padStart(2, '0')}:00</th>
                <td>{formatPercent(row.pct_on_time, language, 1)}</td>
                <td>{formatPercent(row.pct_over_5_min_late, language, 1)}</td>
                <td>{formatMinutes(row.median_delay_min, language, 2)}</td>
                <td>{formatMinutes(row.p90_delay_min, language, 2)}</td>
                <td>{formatNumber(row.trip_count, language)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
