import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import {
  formatAlertPriority,
  formatInterval,
  formatNumber,
  formatPercent,
  formatSigned,
} from '../format';
import { dataLabel, t } from '../i18n';
import { textWidth, wrapText } from '../measure';
import { plotStyle, theme } from '../theme';
import type { AlertInventory, AlertLift, CancellationSummary, Language } from '../types';
import { ChartLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';

interface AlertChartProps {
  language: Language;
  rows: AlertLift[];
  inventory: AlertInventory;
  cancellations: CancellationSummary;
}

function effectColor(row: AlertLift) {
  if ((row.late5_lift_lo ?? 0) > 0) return theme.punctuality.lateStrong;
  if ((row.late5_lift_hi ?? 0) < 0) return theme.early;
  return theme.deemphasis;
}

export function AlertChart({ language, rows, inventory, cancellations }: AlertChartProps) {
  const copy = t(language);
  const pp = language === 'fi' ? '%-yks.' : 'pp';
  const label = useCallback(
    (row: AlertLift) =>
      `${dataLabel(language, 'alertEffect', row.effect)} · ${dataLabel(language, 'alertScope', row.alert_scope)} · ${copy.alertPriority.toLowerCase()} ${formatAlertPriority(row.priority, copy.alertPriorityUnknown)}`,
    [copy.alertPriority, copy.alertPriorityUnknown, language],
  );
  const shortLabel = useCallback(
    (row: AlertLift) =>
      `${dataLabel(language, 'alertEffect', row.effect)} · ${dataLabel(language, 'alertScope', row.alert_scope).toLowerCase()} · ${language === 'fi' ? 'prior.' : 'prio.'} ${formatAlertPriority(row.priority, copy.alertPriorityUnknown)}`,
    [copy.alertPriorityUnknown, language],
  );
  const ordered = useMemo(
    () => [...rows].sort((a, b) => b.late5_lift_pp - a.late5_lift_pp),
    [rows],
  );
  // Keep lines tied with the fifth-largest count so the cut-off does not pick between equals.
  const topCancelled = useMemo(() => {
    const sorted = [...cancellations.by_line].sort(
      (a, b) => b.cancelled_departures - a.cancelled_departures,
    );
    const cutoff = sorted[Math.min(4, sorted.length - 1)]?.cancelled_departures ?? 0;
    return sorted.filter((row) => row.cancelled_departures >= cutoff);
  }, [cancellations.by_line]);
  const render = useCallback(
    (width: number) => {
      const bounds = ordered.flatMap((row) => [
        row.late5_lift_lo ?? row.late5_lift_pp,
        row.late5_lift_hi ?? row.late5_lift_pp,
      ]);
      const low = Math.floor(Math.min(-1, ...bounds));
      const high = Math.ceil(Math.max(1, ...bounds));
      const labelWidth = textWidth(ordered.map(label));
      const valueWidth = textWidth(
        ordered.map((row) => formatSigned(row.late5_lift_pp, language, 1)),
      );
      const marginRight = valueWidth + 18;
      // When the full labels would take more than ~40% of the width, each row's label sits
      // above its interval instead, wrapped to the space available.
      const stacked = width < 560 || labelWidth + 16 > width * 0.4;
      const wrapped = new Map(
        ordered.map((row) => [row, wrapText(shortLabel(row), width - marginRight - 12)]),
      );
      const lineCount = stacked ? Math.max(1, ...[...wrapped.values()].map((l) => l.length)) : 1;
      const rowLabel = stacked ? shortLabel : label;
      const rowHeight = stacked ? 32 + lineCount * 20 : 36;
      const marginTop = stacked ? 10 + lineCount * 20 : 14;
      const marginBottom = 44;
      return Plot.plot({
        width,
        height: marginTop + marginBottom + ordered.length * rowHeight,
        marginTop,
        marginRight,
        marginBottom,
        marginLeft: stacked ? 12 : labelWidth + 16,
        style: plotStyle,
        x: {
          domain: [low, high],
          grid: true,
          label: `${copy.lift}, ${pp}`,
          tickFormat: (value: number) => formatSigned(value, language, 0),
        },
        y: stacked
          ? { domain: ordered.map(rowLabel), axis: null }
          : { domain: ordered.map(rowLabel), label: null, tickSize: 0 },
        marks: [
          Plot.ruleX([0], { stroke: theme.baseline, strokeWidth: 1.5 }),
          Plot.link(
            ordered.filter((row) => row.late5_lift_lo !== null && row.late5_lift_hi !== null),
            {
              x1: 'late5_lift_lo',
              x2: 'late5_lift_hi',
              y: rowLabel,
              stroke: effectColor,
              strokeWidth: 2,
            },
          ),
          Plot.dot(ordered, {
            x: 'late5_lift_pp',
            y: rowLabel,
            r: 5,
            fill: effectColor,
            stroke: theme.surface,
            strokeWidth: 2,
            channels: {
              during: {
                value: (row: AlertLift) =>
                  formatPercent(row.pct_over_5_min_late_alert, language, 1),
                label: copy.alertDuring,
              },
              control: {
                value: (row: AlertLift) =>
                  formatPercent(row.pct_over_5_min_late_control, language, 1),
                label: copy.alertControl,
              },
              notices: { value: 'alert_count', label: copy.alertCount },
            },
            tip: {
              format: {
                x: false,
                fill: false,
                y: true,
                during: true,
                control: true,
                notices: true,
              },
            },
          }),
          ...(stacked
            ? [
                Plot.text(ordered, {
                  frameAnchor: 'left',
                  y: rowLabel,
                  text: (row: AlertLift) => (wrapped.get(row) ?? [rowLabel(row)]).join('\n'),
                  lineAnchor: 'bottom',
                  lineHeight: 1.25,
                  dy: -12,
                  textAnchor: 'start',
                  fill: theme.inkSoft,
                  // A surface halo keeps the label readable where grid lines cross it.
                  stroke: theme.surface,
                  strokeWidth: 4,
                  paintOrder: 'stroke',
                }),
              ]
            : []),
          Plot.text(ordered, {
            x: (row: AlertLift) => row.late5_lift_hi ?? row.late5_lift_pp,
            y: rowLabel,
            text: (row: AlertLift) => `${formatSigned(row.late5_lift_pp, language, 1)}`,
            dx: 10,
            textAnchor: 'start',
            fill: theme.ink,
          }),
        ],
      });
    },
    [copy, label, language, ordered, pp, shortLabel],
  );
  return (
    <>
      <PlotFigure
        title={copy.alertChart}
        subtitle={copy.alertChartSub}
        render={render}
        legend={
          <ChartLegend
            label={copy.alertChart}
            items={[
              { label: copy.clearEffect, color: theme.punctuality.lateStrong, shape: 'dot' },
              { label: copy.unclearEffect, color: theme.deemphasis, shape: 'dot' },
            ]}
          />
        }
        footnote={copy.alertPriorityDescription}
      >
        <div className="table-scroll">
          <table>
            <caption className="sr-only">{copy.tableCaption}</caption>
            <thead>
              <tr>
                <th scope="col">{copy.alert}</th>
                <th scope="col">{copy.alertCount}</th>
                <th scope="col">{copy.alertBuckets}</th>
                <th scope="col">{copy.alertDuring}</th>
                <th scope="col">{copy.alertControl}</th>
                <th scope="col">{copy.lift}</th>
              </tr>
            </thead>
            <tbody>
              {ordered.map((row) => (
                <tr key={`${row.cause}-${row.effect}-${row.alert_scope}-${row.priority}`}>
                  <th scope="row">
                    {dataLabel(language, 'alertCause', row.cause)} · {label(row)}
                  </th>
                  <td>{formatNumber(row.alert_count, language)}</td>
                  <td>{formatNumber(row.bucket_count_alert, language)}</td>
                  <td>{formatPercent(row.pct_over_5_min_late_alert, language, 1)}</td>
                  <td>{formatPercent(row.pct_over_5_min_late_control, language, 1)}</td>
                  <td>
                    {formatSigned(row.late5_lift_pp, language, 1)} {pp}
                    {formatInterval(row.late5_lift_lo, row.late5_lift_hi, language, 'pp')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </PlotFigure>
      <div className="note-grid">
        <p className="definition-note">
          <span aria-hidden="true">i</span>
          {copy.inventoryNote(inventory.included, inventory.long_running, inventory.no_targets)}
        </p>
        <p className="definition-note">
          <span aria-hidden="true">i</span>
          {copy.cancellationsNote(cancellations.total_departures)}
          {topCancelled.length
            ? ` ${copy.cancellationsTop}: ${topCancelled
                .map(
                  (row) =>
                    `${copy.line.toLowerCase()} ${row.line_ref} (${row.cancelled_departures})`,
                )
                .join(', ')}.`
            : null}
        </p>
      </div>
    </>
  );
}
