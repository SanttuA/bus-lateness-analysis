import { lazy, Suspense, useEffect, useMemo, useState } from 'react';

import { loadStops } from '../data';
import { formatMinutes, formatNumber } from '../format';
import { t } from '../i18n';
import { selectStopPoints } from '../model';
import type { DayFilter, DelayDirection, Language, SearchState, StopsPayload } from '../types';

/* oxlint-disable jsx-a11y/no-noninteractive-tabindex -- The stop table's scrollable region must be keyboard-focusable for WCAG 2.1.1. */

const StopMap = lazy(() => import('./StopMap'));

interface StopExplorerProps {
  language: Language;
  search: SearchState;
  onSearchChange: (patch: Partial<SearchState>) => void;
}

export default function StopExplorer({ language, search, onSearchChange }: StopExplorerProps) {
  const copy = t(language);
  const [data, setData] = useState<StopsPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    loadStops()
      .then(setData)
      .catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : String(reason));
      });
  }, []);
  const line = search.line ?? 'all';
  const day: DayFilter = search.day ?? 'all';
  const mode: DelayDirection = search.metric ?? 'late';
  const view = search.view ?? 'map';
  const lines = useMemo(
    () =>
      data
        ? [
            ...new Set(
              data.metrics.filter((row) => row.line_ref !== 'all').map((row) => row.line_ref),
            ),
          ].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
        : [],
    [data],
  );
  const points = useMemo(() => {
    if (!data) return [];
    return selectStopPoints(data, {
      line,
      day,
      mode,
      query: search.stop ?? '',
      locale: language === 'fi' ? 'fi-FI' : 'en-GB',
    });
  }, [data, day, language, line, mode, search.stop]);

  if (error)
    return (
      <p className="empty-state" role="alert">
        {copy.loadError} {error}
      </p>
    );
  if (!data) return <output className="empty-state">{copy.loading}</output>;
  return (
    <div className="explorer-panel stop-explorer">
      <div className="filter-grid stop-filters">
        <label>
          <span>{copy.line}</span>
          <select value={line} onChange={(event) => onSearchChange({ line: event.target.value })}>
            <option value="all">{copy.allLines}</option>
            {lines.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>{copy.dayType}</span>
          <select
            value={day}
            onChange={(event) => {
              const value = event.target.value;
              onSearchChange({
                day: value === 'all' ? undefined : (value as 'weekday' | 'weekend'),
              });
            }}
          >
            <option value="all">{copy.allDays}</option>
            <option value="weekday">{copy.weekday}</option>
            <option value="weekend">{copy.weekend}</option>
          </select>
        </label>
        <fieldset>
          <legend>{copy.metric}</legend>
          <div className="segmented">
            {(['late', 'early'] as const).map((value) => (
              <button
                key={value}
                type="button"
                aria-pressed={mode === value}
                onClick={() => onSearchChange({ metric: value })}
              >
                {value === 'late' ? copy.late : copy.early}
              </button>
            ))}
          </div>
        </fieldset>
        <label className="search-field">
          <span>{copy.stopSearch}</span>
          <input
            type="search"
            value={search.stop ?? ''}
            onChange={(event) => onSearchChange({ stop: event.target.value || undefined })}
          />
        </label>
        <fieldset>
          <legend>{language === 'fi' ? 'Esitystapa' : 'View'}</legend>
          <div className="segmented">
            {(['map', 'table'] as const).map((value) => (
              <button
                key={value}
                type="button"
                aria-pressed={view === value}
                onClick={() => onSearchChange({ view: value })}
              >
                {value === 'map' ? copy.mapView : copy.tableView}
              </button>
            ))}
          </div>
        </fieldset>
      </div>
      <p className="result-count" aria-live="polite">
        {formatNumber(points.length, language)}{' '}
        {language === 'fi' ? 'pysäkkiä rajauksessa' : 'stops in selection'}
      </p>
      {points.length === 0 ? <p className="empty-state">{copy.noData}</p> : null}
      {view === 'map' && points.length ? (
        <Suspense fallback={<p className="empty-state">{copy.loading}</p>}>
          <StopMap language={language} mode={mode} points={points} />
        </Suspense>
      ) : null}
      {view === 'table' && points.length ? (
        <section
          className="table-scroll stop-table"
          aria-label={`${copy.tableCaption} · ${language === 'fi' ? '50 korkeinta arvoa' : 'top 50 values'}`}
          tabIndex={0}
        >
          <table>
            <caption>
              {copy.tableCaption} · {language === 'fi' ? '50 korkeinta arvoa' : 'top 50 values'}
            </caption>
            <thead>
              <tr>
                <th scope="col">{copy.stop}</th>
                <th scope="col">ID</th>
                <th scope="col">{mode === 'late' ? copy.p90 : copy.earlyP90}</th>
                <th scope="col">{copy.buckets}</th>
                <th scope="col">{copy.linesServed}</th>
              </tr>
            </thead>
            <tbody>
              {points.slice(0, 50).map((point) => (
                <tr key={point.stop_id}>
                  <th scope="row">{point.stop_name}</th>
                  <td>{point.stop_id}</td>
                  <td>{formatMinutes(point.display_value, language, 2)}</td>
                  <td>{formatNumber(point.bucket_count, language)}</td>
                  <td>{formatNumber(point.line_count, language)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}
    </div>
  );
}
