import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { labelStep, textWidth } from '../measure';
import { selectDefaultLine, selectExplorableLines, selectHeatmapCells } from '../model';
import type { HeatmapCell } from '../model';
import { plotStyle, theme } from '../theme';
import type { DelayDirection, Language, LinesPayload, SearchState } from '../types';
import { BinLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';
import { SegmentedControl } from './SegmentedControl';

interface LineExplorerProps {
  language: Language;
  data: LinesPayload;
  search: SearchState;
  onSearchChange: (patch: Partial<SearchState>) => void;
}

export function LineExplorer({ language, data, search, onSearchChange }: LineExplorerProps) {
  const copy = t(language);
  const lineRefs = useMemo(() => selectExplorableLines(data), [data]);
  const defaultLine = useMemo(() => selectDefaultLine(data), [data]);
  const line = search.line && lineRefs.includes(search.line) ? search.line : defaultLine;
  const mode: DelayDirection = search.heat ?? 'late';
  const lineRecord = data.lines.find((row) => row.line_ref === line);
  const { rows, cells } = useMemo(
    () => (line ? selectHeatmapCells(data, line, mode) : { rows: [], cells: [] }),
    [data, line, mode],
  );
  // Row label layouts from widest to most compact; render picks the first that fits.
  const labelFormats = useMemo(() => {
    const day = (value: string, short: boolean) =>
      value === 'weekday'
        ? short
          ? copy.weekdayShort
          : copy.weekday
        : short
          ? copy.weekendShort
          : copy.weekend;
    return {
      preferred: [
        (direction: string, value: string) =>
          `${copy.direction} ${direction} · ${day(value, false)}`,
        (direction: string, value: string) =>
          `${copy.direction} ${direction}\n${day(value, false)}`,
      ],
      compact: (direction: string, value: string) =>
        `${copy.direction} ${direction}\n${day(value, true)}`,
    };
  }, [copy]);
  const ramp = mode === 'late' ? theme.lateRamp : theme.earlyRamp;
  const maxValue = useMemo(() => Math.max(5, ...cells.map((cell) => cell.value ?? 0)), [cells]);
  const metricLabel = mode === 'late' ? copy.heatmapLate : copy.heatmapEarly;
  const thresholds = useMemo(() => {
    const step = Math.max(1, Math.ceil(maxValue)) / ramp.length;
    return Array.from(
      { length: ramp.length - 1 },
      (_, index) => Math.round(step * (index + 1) * 10) / 10,
    );
  }, [maxValue, ramp.length]);

  const render = useCallback(
    (width: number) => {
      const narrow = width < 560;
      const filled = cells.filter((cell) => cell.value !== null);
      const fits = (format: typeof labelFormats.compact) =>
        textWidth(rows.map((row) => format(row.direction_ref, row.day_type))) + 12 <= width * 0.4;
      const rowLabel = labelFormats.preferred.find(fits) ?? labelFormats.compact;
      const labels = rows.map((row) => rowLabel(row.direction_ref, row.day_type));
      const twoLine = labels.some((text) => text.includes('\n'));
      const marginLeft = textWidth(labels) + 12;
      const hourStep = labelStep((width - marginLeft - 8) / 24, textWidth(['23']));
      return Plot.plot({
        width,
        height: 70 + rows.length * (twoLine ? 46 : narrow ? 30 : 36),
        marginTop: 8,
        marginRight: 8,
        marginBottom: 40,
        marginLeft,
        padding: 0.06,
        style: plotStyle,
        x: {
          type: 'band',
          domain: Array.from({ length: 24 }, (_, hour) => hour),
          label: copy.hour,
          tickFormat: (hour: number) => (hour % hourStep === 0 ? `${hour}` : ''),
          tickSize: 0,
        },
        y: {
          domain: rows.map((row) => rowLabel(row.direction_ref, row.day_type)),
          label: null,
          tickSize: 0,
        },
        color: { type: 'threshold', domain: thresholds, range: [...ramp] },
        marks: [
          Plot.cell(cells, {
            x: 'local_hour',
            y: (cell: HeatmapCell) => rowLabel(cell.direction_ref, cell.day_type),
            fill: theme.band,
            rx: 3,
          }),
          Plot.cell(filled, {
            x: 'local_hour',
            y: (cell: HeatmapCell) => rowLabel(cell.direction_ref, cell.day_type),
            fill: 'value',
            rx: 3,
            channels: {
              share: {
                value: (cell: HeatmapCell) => formatPercent(cell.value ?? 0, language, 1),
                label: metricLabel,
              },
              trips: {
                value: (cell: HeatmapCell) => cell.context?.trip_count ?? 0,
                label: copy.trips,
              },
            },
            tip: { format: { fill: false, x: true, y: true, share: true, trips: true } },
          }),
        ],
      });
    },
    [cells, copy, labelFormats, language, metricLabel, ramp, rows, thresholds],
  );

  if (!line) return null;
  const directionLabels = lineRecord?.direction_labels ?? {};
  return (
    <div className="explorer-panel">
      <div className="filter-row">
        <label className="select-field">
          <span>{copy.line}</span>
          <select value={line} onChange={(event) => onSearchChange({ line: event.target.value })}>
            {lineRefs.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <SegmentedControl
          legend={copy.metric}
          value={mode}
          options={[
            { value: 'late', label: copy.heatmapLate },
            { value: 'early', label: copy.heatmapEarly },
          ]}
          onChange={(value) => onSearchChange({ heat: value as DelayDirection })}
        />
      </div>
      {Object.keys(directionLabels).length ? (
        <ul className="direction-list">
          {Object.entries(directionLabels).map(([direction, destination]) => (
            <li key={direction}>
              <strong>
                {copy.direction} {direction}
              </strong>{' '}
              {copy.towards} {destination}
            </li>
          ))}
        </ul>
      ) : null}
      {cells.some((cell) => cell.value !== null) ? (
        <PlotFigure
          className="heatmap-figure"
          title={`${copy.line} ${lineRecord?.line_name ?? line}`}
          subtitle={`${metricLabel} · ${formatNumber(lineRecord?.trip_count ?? 0, language)} ${copy.trips.toLowerCase()}`}
          render={render}
          legend={
            <BinLegend
              language={language}
              title={metricLabel}
              thresholds={thresholds}
              ramp={ramp}
            />
          }
        >
          <div className="table-scroll">
            <table>
              <caption className="sr-only">{copy.tableCaption}</caption>
              <thead>
                <tr>
                  <th scope="col">{copy.direction}</th>
                  <th scope="col">{copy.dayType}</th>
                  <th scope="col">{copy.hour}</th>
                  <th scope="col">{copy.onTime}</th>
                  <th scope="col">{copy.overFive}</th>
                  <th scope="col">{copy.overThreeEarly}</th>
                  <th scope="col">{copy.p90}</th>
                  <th scope="col">{copy.trips}</th>
                </tr>
              </thead>
              <tbody>
                {cells
                  .filter((cell) => cell.context !== null)
                  .map((cell) => {
                    const context = cell.context!;
                    return (
                      <tr key={`${cell.key}-${cell.local_hour}`}>
                        <td>{cell.direction_ref}</td>
                        <td>{cell.day_type === 'weekday' ? copy.weekday : copy.weekend}</td>
                        <th scope="row">{String(cell.local_hour).padStart(2, '0')}:00</th>
                        <td>{formatPercent(context.pct_on_time, language, 1)}</td>
                        <td>{formatPercent(context.pct_over_5_min_late, language, 1)}</td>
                        <td>{formatPercent(context.pct_over_3_min_early, language, 1)}</td>
                        <td>{formatMinutes(context.p90_delay_min, language, 2)}</td>
                        <td>{formatNumber(context.trip_count, language)}</td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        </PlotFigure>
      ) : (
        <output className="empty-state">{copy.noData}</output>
      )}
    </div>
  );
}
