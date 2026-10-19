import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatDate, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { labelStep, textWidth } from '../measure';
import { plotStyle, theme } from '../theme';
import type { DailyMetric, Language } from '../types';
import { ChartLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';

/** Below this collector coverage a day's values are shown but not joined into the line. */
const LINE_COVERAGE_PCT = 50;
const FULL_COVERAGE_PCT = 95;
const DAY_MS = 24 * 60 * 60 * 1000;

interface DailyPoint {
  date: Date;
  row: DailyMetric;
  series: 'late' | 'early';
  value: number;
  partial: boolean;
}

function shortDate(value: Date, language: Language) {
  return new Intl.DateTimeFormat(language === 'fi' ? 'fi-FI' : 'en-GB', {
    day: 'numeric',
    month: language === 'fi' ? 'numeric' : 'short',
    timeZone: 'UTC',
  }).format(value);
}

function toDate(value: string) {
  return new Date(`${value}T12:00:00Z`);
}

/** Monday dates (noon UTC, like the data points) inside the domain. */
function mondays(start: Date, end: Date): Date[] {
  let first = Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate(), 12);
  if (first < start.getTime()) first += DAY_MS;
  // getUTCDay: Sunday = 0, Monday = 1.
  first += ((8 - new Date(first).getUTCDay()) % 7) * DAY_MS;
  const count = Math.max(0, Math.floor((end.getTime() - first) / (7 * DAY_MS)) + 1);
  return Array.from({ length: count }, (_, index) => new Date(first + index * 7 * DAY_MS));
}

/**
 * Keeps the coverage labels that do not collide, lowest coverage first, so the worst gaps
 * stay labelled on narrow screens. Each label is centred 8px above its bar and is ~16px tall.
 */
function placeCoverageLabels(
  rows: DailyMetric[],
  position: {
    x: (row: DailyMetric) => number;
    y: (row: DailyMetric) => number;
    width: (row: DailyMetric) => number;
  },
): DailyMetric[] {
  const kept: { row: DailyMetric; left: number; right: number; top: number; bottom: number }[] = [];
  const byPriority = [...rows].sort(
    (a, b) => (a.collector_coverage_pct ?? 0) - (b.collector_coverage_pct ?? 0),
  );
  for (const row of byPriority) {
    const x = position.x(row);
    const half = position.width(row) / 2 + 2;
    const centre = position.y(row) - 8;
    const box = { row, left: x - half, right: x + half, top: centre - 8, bottom: centre + 8 };
    const clash = kept.some(
      (other) =>
        box.left < other.right &&
        other.left < box.right &&
        box.top < other.bottom &&
        other.top < box.bottom,
    );
    if (!clash) kept.push(box);
  }
  return kept.map((box) => box.row);
}

export function DailyTrendChart({ language, rows }: { language: Language; rows: DailyMetric[] }) {
  const copy = t(language);
  const series = useMemo(
    () => ({
      late: { label: copy.overFive, color: theme.late },
      early: { label: copy.overThreeEarly, color: theme.early },
    }),
    [copy.overFive, copy.overThreeEarly],
  );
  const points = useMemo(() => {
    const result: DailyPoint[] = [];
    for (const row of rows) {
      const coverage = row.collector_coverage_pct ?? 100;
      for (const key of ['late', 'early'] as const) {
        result.push({
          date: toDate(row.local_date),
          row,
          series: key,
          value: key === 'late' ? row.pct_over_5_min_late : row.pct_over_3_min_early,
          partial: coverage < FULL_COVERAGE_PCT,
        });
      }
    }
    return result;
  }, [rows]);
  const restDays = useMemo(() => rows.filter((row) => row.day_type === 'weekend'), [rows]);

  const render = useCallback(
    (width: number) => {
      const narrow = width < 560;
      const container = document.createElement('div');
      const first = rows.at(0);
      const final = rows.at(-1);
      const xDomain: [Date, Date] | undefined =
        first && final
          ? [
              new Date(toDate(first.local_date).getTime() - DAY_MS / 2),
              new Date(toDate(final.local_date).getTime() + DAY_MS / 2),
            ]
          : undefined;
      const last = (['late', 'early'] as const)
        .map((key) => points.filter((point) => point.series === key && !point.partial).at(-1))
        .filter((point): point is DailyPoint => point !== undefined);
      // Direct series labels need a right margin as wide as the longest label; drop them (the
      // legend names the series) when that would squeeze the chart.
      const directWidth = textWidth(last.map((point) => series[point.series].label)) + 16;
      const directLabels = !narrow && directWidth <= width * 0.3;
      const ticks = xDomain ? mondays(xDomain[0], xDomain[1]) : [];
      const tickWidth = textWidth(ticks.map((value) => shortDate(value, language)));
      const marginLeft = Math.max(52, textWidth(['100 %']) + 14);
      // The last date label is centred on its tick, so half of it can reach past the frame.
      const marginRight = directLabels ? directWidth : Math.ceil(tickWidth / 2) + 6;
      const plotWidth = width - marginLeft - marginRight;
      const span = xDomain ? xDomain[1].getTime() - xDomain[0].getTime() : DAY_MS;
      const weekStep = labelStep((plotWidth * 7 * DAY_MS) / span, tickWidth, [1, 2, 3, 4], 12);
      const tickValues = ticks.filter((_, index) => index % weekStep === 0);
      const common = {
        width,
        marginLeft,
        marginRight,
        style: plotStyle,
      };
      const bands = Plot.rectX(restDays, {
        x1: (row: DailyMetric) => new Date(toDate(row.local_date).getTime() - DAY_MS / 2),
        x2: (row: DailyMetric) => new Date(toDate(row.local_date).getTime() + DAY_MS / 2),
        fill: theme.band,
      });
      const trend = Plot.plot({
        ...common,
        height: narrow ? 300 : 340,
        marginTop: 16,
        marginBottom: 30,
        x: {
          type: 'utc',
          domain: xDomain,
          label: null,
          ticks: tickValues,
          tickFormat: (value: Date) => shortDate(value, language),
        },
        y: {
          label: '%',
          grid: true,
          zero: true,
          tickFormat: (value: number) => formatNumber(value, language, 0),
        },
        marks: [
          bands,
          Plot.ruleY([0], { stroke: theme.baseline }),
          Plot.lineY(points, {
            x: 'date',
            y: (point: DailyPoint) =>
              (point.row.collector_coverage_pct ?? 100) < LINE_COVERAGE_PCT ? null : point.value,
            z: 'series',
            stroke: (point: DailyPoint) => series[point.series].color,
            strokeWidth: 2,
          }),
          Plot.dot(points, {
            x: 'date',
            y: 'value',
            r: 4,
            fill: (point: DailyPoint) =>
              point.partial ? theme.surface : series[point.series].color,
            stroke: (point: DailyPoint) =>
              point.partial ? series[point.series].color : theme.surface,
            strokeWidth: 2,
          }),
          ...(!directLabels
            ? []
            : [
                Plot.text(last, {
                  x: 'date',
                  y: 'value',
                  text: (point: DailyPoint) => series[point.series].label,
                  dx: 10,
                  textAnchor: 'start',
                  fill: theme.inkSoft,
                }),
              ]),
          Plot.ruleX(points, Plot.pointerX({ x: 'date', stroke: theme.muted, strokeWidth: 1 })),
          Plot.tip(
            points.filter((point) => point.series === 'late'),
            Plot.pointerX({
              x: 'date',
              y: 'value',
              title: (point: DailyPoint) =>
                [
                  `${formatPercent(point.row.pct_over_5_min_late, language, 1)} · ${copy.overFive}`,
                  `${formatPercent(point.row.pct_over_3_min_early, language, 1)} · ${copy.overThreeEarly}`,
                  `${formatPercent(point.row.pct_on_time, language, 1)} · ${copy.onTime.toLowerCase()}`,
                  `${copy.coverage}: ${formatPercent(point.row.collector_coverage_pct ?? 0, language, 0)}`,
                  `${formatDate(point.row.local_date, language)}${point.row.is_holiday ? ` (${copy.holiday})` : ''}`,
                ].join('\n'),
            }),
          ),
        ],
      });
      const coverageHeight = 120;
      const coverageMargins = { top: 10, bottom: 36 };
      const coverageLabels = placeCoverageLabels(
        rows.filter((row) => (row.collector_coverage_pct ?? 0) < FULL_COVERAGE_PCT),
        {
          x: (row) =>
            xDomain
              ? marginLeft +
                ((toDate(row.local_date).getTime() - xDomain[0].getTime()) / span) * plotWidth
              : 0,
          y: (row) =>
            coverageMargins.top +
            (1 - (row.collector_coverage_pct ?? 0) / 100) *
              (coverageHeight - coverageMargins.top - coverageMargins.bottom),
          width: (row) => textWidth([formatPercent(row.collector_coverage_pct ?? 0, language, 0)]),
        },
      );
      const coverage = Plot.plot({
        ...common,
        height: coverageHeight,
        marginTop: coverageMargins.top,
        marginBottom: coverageMargins.bottom,
        x: {
          type: 'utc',
          domain: xDomain,
          label: null,
          ticks: tickValues,
          tickFormat: (value: Date) => shortDate(value, language),
        },
        y: {
          domain: [0, 100],
          ticks: [0, 50, 100],
          label: null,
          grid: true,
          tickFormat: (value: number) => `${value} %`,
        },
        marks: [
          bands,
          Plot.rectY(rows, {
            x1: (row: DailyMetric) => new Date(toDate(row.local_date).getTime() - DAY_MS * 0.38),
            x2: (row: DailyMetric) => new Date(toDate(row.local_date).getTime() + DAY_MS * 0.38),
            y: (row: DailyMetric) => row.collector_coverage_pct ?? 0,
            fill: (row: DailyMetric) =>
              (row.collector_coverage_pct ?? 0) < FULL_COVERAGE_PCT ? theme.ink : theme.neutral,
            rx: 2,
          }),
          Plot.text(coverageLabels, {
            x: (row: DailyMetric) => toDate(row.local_date),
            y: (row: DailyMetric) => row.collector_coverage_pct ?? 0,
            text: (row: DailyMetric) => formatPercent(row.collector_coverage_pct ?? 0, language, 0),
            dy: -8,
            fill: theme.ink,
            // A surface halo keeps neighbouring labels and bars apart.
            stroke: theme.surface,
            strokeWidth: 4,
            paintOrder: 'stroke',
          }),
        ],
      });
      const heading = document.createElement('p');
      heading.className = 'subplot-title';
      heading.textContent = `${copy.coverageChart} · ${copy.coverageChartSub}`;
      container.append(trend, heading, coverage);
      return container;
    },
    [copy, language, points, restDays, rows, series],
  );

  return (
    <PlotFigure
      title={copy.dailyChart}
      subtitle={copy.dailyChartSub}
      render={render}
      legend={
        <ChartLegend
          label={copy.dailyChart}
          items={[
            { label: series.late.label, color: series.late.color, shape: 'line' },
            { label: series.early.label, color: series.early.color, shape: 'line' },
            { label: copy.weekend, color: theme.band, shape: 'rect' },
          ]}
        />
      }
    >
      <div className="table-scroll">
        <table>
          <caption className="sr-only">{copy.tableCaption}</caption>
          <thead>
            <tr>
              <th scope="col">{copy.date}</th>
              <th scope="col">{copy.dayType}</th>
              <th scope="col">{copy.onTime}</th>
              <th scope="col">{copy.overFive}</th>
              <th scope="col">{copy.overThreeEarly}</th>
              <th scope="col">{copy.trips}</th>
              <th scope="col">{copy.coverage}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.local_date}>
                <th scope="row">{formatDate(row.local_date, language)}</th>
                <td>
                  {row.day_type === 'weekday' ? copy.weekday : copy.weekend}
                  {row.is_holiday ? ` (${copy.holiday})` : ''}
                </td>
                <td>{formatPercent(row.pct_on_time, language, 1)}</td>
                <td>{formatPercent(row.pct_over_5_min_late, language, 1)}</td>
                <td>{formatPercent(row.pct_over_3_min_early, language, 1)}</td>
                <td>{formatNumber(row.trip_count, language)}</td>
                <td>{formatPercent(row.collector_coverage_pct ?? 0, language, 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlotFigure>
  );
}
