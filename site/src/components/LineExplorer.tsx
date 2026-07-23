import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { selectDefaultLine, selectExplorableLines, selectLineContexts } from '../model';
import { normalizeHourRange } from '../search';
import type { Language, LinesPayload, SearchState } from '../types';
import { PlotFigure } from './PlotFigure';

interface LineExplorerProps {
  language: Language;
  data: LinesPayload;
  search: SearchState;
  onSearchChange: (patch: Partial<SearchState>) => void;
}

export function LineExplorer({ language, data, search, onSearchChange }: LineExplorerProps) {
  const copy = t(language);
  const lineRefs = useMemo(() => selectExplorableLines(data), [data]);
  const direction = search.direction ?? '1';
  const day = search.day ?? 'weekday';
  const defaultLine = useMemo(
    () => selectDefaultLine(data, direction, day),
    [data, day, direction],
  );
  const line = search.line && lineRefs.includes(search.line) ? search.line : defaultLine;
  const { start, end } = normalizeHourRange(search.from, search.to);
  const rows = useMemo(
    () => (line ? selectLineContexts(data, line, direction, day, start, end) : []),
    [data, day, direction, end, line, start],
  );
  const render = useCallback(
    (width: number) =>
      Plot.plot({
        width,
        height: width < 560 ? 380 : 430,
        marginTop: 16,
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
          domain: [start, end],
          ticks: width < 560 ? 6 : 12,
        },
        y: { label: language === 'fi' ? 'viive, min' : 'delay, min', grid: true },
        marks: [
          Plot.ruleY([0], { stroke: '#737b79' }),
          Plot.lineY(rows, {
            x: 'local_hour',
            y: 'p90_delay_min',
            stroke: '#b85b1b',
            strokeWidth: 3,
            tip: true,
          }),
          Plot.dot(rows, { x: 'local_hour', y: 'p90_delay_min', fill: '#b85b1b', r: 4 }),
          Plot.lineY(rows, {
            x: 'local_hour',
            y: 'median_delay_min',
            stroke: '#316f9e',
            strokeWidth: 2,
            strokeDasharray: '5,4',
            tip: true,
          }),
        ],
      }),
    [end, language, rows, start],
  );

  if (!line) return null;
  return (
    <div className="explorer-panel">
      <div className="filter-grid" aria-label={copy.explorerTitle}>
        <label>
          <span>{copy.line}</span>
          <select value={line} onChange={(event) => onSearchChange({ line: event.target.value })}>
            {lineRefs.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>{copy.direction}</span>
          <select
            value={direction}
            onChange={(event) => onSearchChange({ direction: event.target.value as '1' | '2' })}
          >
            <option value="1">1</option>
            <option value="2">2</option>
          </select>
        </label>
        <label>
          <span>{copy.dayType}</span>
          <select
            value={day}
            onChange={(event) =>
              onSearchChange({ day: event.target.value as 'weekday' | 'weekend' })
            }
          >
            <option value="weekday">{copy.weekday}</option>
            <option value="weekend">{copy.weekend}</option>
          </select>
        </label>
        <label>
          <span>
            {copy.fromHour}: {String(start).padStart(2, '0')}:00
          </span>
          <input
            type="range"
            min="0"
            max="23"
            value={start}
            onChange={(event) => {
              const value = Number(event.target.value);
              onSearchChange({ from: value, ...(value > end ? { to: value } : {}) });
            }}
          />
        </label>
        <label>
          <span>
            {copy.toHour}: {String(end).padStart(2, '0')}:00
          </span>
          <input
            type="range"
            min="0"
            max="23"
            value={end}
            onChange={(event) => {
              const value = Number(event.target.value);
              onSearchChange({ to: value, ...(value < start ? { from: value } : {}) });
            }}
          />
        </label>
      </div>
      {rows.length ? (
        <PlotFigure
          title={`${copy.line} ${line}`}
          subtitle={`${direction} · ${day === 'weekday' ? copy.weekday : copy.weekend} · ${String(start).padStart(2, '0')}:00–${String(end).padStart(2, '0')}:00`}
          render={render}
        >
          <div className="table-scroll">
            <table>
              <caption className="sr-only">{copy.tableCaption}</caption>
              <thead>
                <tr>
                  <th scope="col">{language === 'fi' ? 'Tunti' : 'Hour'}</th>
                  <th scope="col">{copy.median}</th>
                  <th scope="col">{copy.p90}</th>
                  <th scope="col">{copy.overFive}</th>
                  <th scope="col">{copy.buckets}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.local_hour}>
                    <th scope="row">{String(row.local_hour).padStart(2, '0')}:00</th>
                    <td>{formatMinutes(row.median_delay_min, language, 2)}</td>
                    <td>{formatMinutes(row.p90_delay_min, language, 2)}</td>
                    <td>{formatPercent(row.pct_over_5_min_late, language, 2)}</td>
                    <td>{formatNumber(row.bucket_count, language)}</td>
                  </tr>
                ))}
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
