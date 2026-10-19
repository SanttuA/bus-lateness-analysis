import * as Plot from '@observablehq/plot';
import { useCallback, useMemo } from 'react';

import { formatInterval, formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { punctualitySegments, selectRankedLines } from '../model';
import type { PunctualityBand } from '../model';
import { textWidth } from '../measure';
import { plotStyle, theme } from '../theme';
import type { DelayDirection, Language, LineMetric, LineScope, SearchState } from '../types';
import { ChartLegend } from './ChartLegend';
import { PlotFigure } from './PlotFigure';
import { SegmentedControl } from './SegmentedControl';

interface PunctualityChartProps {
  language: Language;
  lines: LineMetric[];
  frequentTripsPerDay: number;
  search: SearchState;
  onSearchChange: (patch: Partial<SearchState>) => void;
}

const BAND_COLORS: Record<PunctualityBand, string> = theme.punctuality;
const ROW_HEIGHT = 34;

function symmetricTicks(extent: number, step: number): number[] {
  const count = Math.floor(extent / step);
  return Array.from({ length: 2 * count + 1 }, (_, index) => (index - count) * step);
}

export function PunctualityChart({
  language,
  lines,
  frequentTripsPerDay,
  search,
  onSearchChange,
}: PunctualityChartProps) {
  const copy = t(language);
  const scope: LineScope = search.lines ?? 'frequent';
  const mode: DelayDirection = search.rank ?? 'late';
  const rows = useMemo(
    () => selectRankedLines(lines, scope, mode, frequentTripsPerDay),
    [frequentTripsPerDay, lines, mode, scope],
  );
  const label = useCallback(
    (row: LineMetric) => (row.low_quality ? `${row.line_name} *` : row.line_name),
    [],
  );
  const segments = useMemo(
    () =>
      punctualitySegments(rows).map((segment) => ({
        ...segment,
        label: label(rows.find((row) => row.line_ref === segment.line_ref) as LineMetric),
      })),
    [label, rows],
  );
  const extent = useMemo(() => {
    const largest = Math.max(
      10,
      ...rows.map((row) => Math.max(row.pct_over_1_min_early, row.pct_over_3_min_late)),
    );
    return Math.ceil(largest / 10) * 10;
  }, [rows]);

  const render = useCallback(
    (width: number) => {
      const narrow = width < 560;
      const valueText = (row: LineMetric) =>
        formatPercent(
          mode === 'late' ? row.pct_over_5_min_late : row.pct_over_3_min_early,
          language,
          1,
        );
      const marginLeft = Math.max(40, textWidth(rows.map(label)) + 14);
      const valueWidth = textWidth(rows.map(valueText), { fontWeight: 600 }) + 10;
      const onTimeWidth = textWidth([
        copy.onTime,
        ...rows.map((row) => formatPercent(row.pct_on_time, language, 0)),
      ]);
      const showOnTime = !narrow && onTimeWidth + 16 <= width * 0.2;
      const marginRight = showOnTime ? onTimeWidth + 16 : 12;
      // Room inside the frame for the value labels on the side the lines are ranked by, so a
      // long bar's label never runs into the line names or past the edge.
      const plotWidth = width - marginLeft - marginRight;
      const pad = (valueWidth * 2 * extent) / Math.max(1, plotWidth - valueWidth);
      const domain = mode === 'late' ? [-extent, extent + pad] : [-(extent + pad), extent];
      const columnX = domain[1] ?? extent;
      return Plot.plot({
        width,
        height: 56 + rows.length * ROW_HEIGHT,
        marginTop: 30,
        marginRight,
        marginBottom: 40,
        marginLeft,
        style: plotStyle,
        x: {
          domain,
          grid: true,
          // Ticks stay within the bars' own range, not the label room added beside it.
          ticks: symmetricTicks(extent, extent >= (narrow ? 40 : 60) ? 20 : 10),
          tickFormat: (value: number) => `${Math.abs(value)}`,
          label: language === 'fi' ? '← etuajassa · % · myöhässä →' : '← early · % · late →',
          labelAnchor: 'center',
        },
        y: { domain: rows.map(label), label: null, tickSize: 0 },
        marks: [
          Plot.barX(segments, {
            x1: 'x1',
            x2: 'x2',
            y: 'label',
            fill: (segment) => BAND_COLORS[segment.band as PunctualityBand],
            insetTop: (ROW_HEIGHT - 20) / 2,
            insetBottom: (ROW_HEIGHT - 20) / 2,
            insetLeft: 1,
            insetRight: 1,
            channels: {
              band: { value: (segment) => copy.bands[segment.band as PunctualityBand], label: '' },
              share: {
                value: (segment) => formatPercent(segment.share, language, 1),
                label: '',
              },
            },
            tip: {
              format: { x1: false, x2: false, fill: false, y: true, band: true, share: true },
            },
          }),
          Plot.ruleX([0], { stroke: theme.baseline }),
          Plot.text(rows, {
            x: (row: LineMetric) =>
              mode === 'late' ? row.pct_over_3_min_late : -row.pct_over_1_min_early,
            y: label,
            text: valueText,
            dx: mode === 'late' ? 6 : -6,
            textAnchor: mode === 'late' ? 'start' : 'end',
            fill: theme.ink,
            fontWeight: 600,
          }),
          ...(!showOnTime
            ? []
            : [
                Plot.text(rows, {
                  x: columnX,
                  y: label,
                  text: (row: LineMetric) => formatPercent(row.pct_on_time, language, 0),
                  dx: marginRight - 2,
                  textAnchor: 'end',
                  fill: theme.inkSoft,
                }),
                Plot.text([copy.onTime], {
                  x: columnX,
                  frameAnchor: 'top',
                  dx: marginRight - 2,
                  dy: -22,
                  textAnchor: 'end',
                  fill: theme.inkSoft,
                }),
              ]),
        ],
      });
    },
    [copy, extent, label, language, mode, rows, segments],
  );

  const hasLowQuality = rows.some((row) => row.low_quality);
  return (
    <div className="chart-block">
      <div className="filter-row">
        <SegmentedControl
          legend={copy.scopeLabel}
          value={scope}
          options={[
            {
              value: 'frequent',
              label: copy.scopeFrequent,
            },
            { value: 'all', label: copy.scopeAll },
          ]}
          onChange={(value) => onSearchChange({ lines: value as LineScope })}
        />
        <SegmentedControl
          legend={copy.sortLabel}
          value={mode}
          options={[
            { value: 'late', label: copy.sortLate },
            { value: 'early', label: copy.sortEarly },
          ]}
          onChange={(value) => onSearchChange({ rank: value as DelayDirection })}
        />
      </div>
      <PlotFigure
        title={copy.punctualityChart}
        subtitle={`${copy.punctualityChartSub}${scope === 'frequent' ? ` · ${copy.scopeFrequent.toLowerCase()} = ≥ ${frequentTripsPerDay} ${copy.tripsPerDay.toLowerCase()}` : ''}`}
        render={render}
        legend={
          <ChartLegend
            label={copy.punctualityChart}
            items={(['earlyStrong', 'earlyMild', 'lateMild', 'lateStrong'] as const).map(
              (band) => ({ label: copy.bands[band], color: BAND_COLORS[band] }),
            )}
          />
        }
        footnote={hasLowQuality ? `* ${copy.lowQuality}: ${copy.lowQualityNote}` : undefined}
      >
        <div className="table-scroll">
          <table>
            <caption className="sr-only">{copy.tableCaption}</caption>
            <thead>
              <tr>
                <th scope="col">{copy.line}</th>
                <th scope="col">{copy.tripsPerDay}</th>
                <th scope="col">{copy.onTime}</th>
                <th scope="col">{copy.overFive}</th>
                <th scope="col">{copy.overThreeEarly}</th>
                <th scope="col">{copy.p90}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.line_ref}>
                  <th scope="row">
                    {row.line_name}
                    {row.low_quality ? ` (${copy.lowQuality})` : ''}
                  </th>
                  <td>{formatNumber(row.trips_per_day, language, 1)}</td>
                  <td>{formatPercent(row.pct_on_time, language, 1)}</td>
                  <td>
                    {formatPercent(row.pct_over_5_min_late, language, 1)}
                    {formatInterval(
                      row.pct_over_5_min_late_ci_lo,
                      row.pct_over_5_min_late_ci_hi,
                      language,
                      '%',
                    )}
                  </td>
                  <td>
                    {formatPercent(row.pct_over_3_min_early, language, 1)}
                    {formatInterval(
                      row.pct_over_3_min_early_ci_lo,
                      row.pct_over_3_min_early_ci_hi,
                      language,
                      '%',
                    )}
                  </td>
                  <td>
                    {formatMinutes(row.p90_delay_min, language, 1)}
                    {formatInterval(row.p90_delay_ci_lo, row.p90_delay_ci_hi, language, 'min')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </PlotFigure>
    </div>
  );
}
