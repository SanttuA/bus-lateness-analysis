import { formatNumber, formatPercent } from '../format';
import type { Language } from '../types';

export interface LegendItem {
  label: string;
  color: string;
  shape?: 'rect' | 'line' | 'dot' | 'ring';
}

/** HTML legend: text stays in ink tokens, the swatch beside it carries the colour. */
export function ChartLegend({ items, label }: { items: LegendItem[]; label?: string }) {
  return (
    <ul className="chart-legend" aria-label={label}>
      {items.map((item) => (
        <li key={item.label}>
          <span
            className={`legend-swatch legend-${item.shape ?? 'rect'}`}
            style={{ '--swatch': item.color } as React.CSSProperties}
            aria-hidden="true"
          />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

/** Legend for a binned (threshold) colour scale: one swatch and range per class. */
export function BinLegend({
  language,
  title,
  note,
  thresholds,
  ramp,
}: {
  language: Language;
  title: string;
  note?: string;
  thresholds: number[];
  ramp: readonly string[];
}) {
  const edges = [0, ...thresholds];
  return (
    <div className="map-legend">
      <p className="map-legend-title">
        {title}
        {note ? <span>{note}</span> : null}
      </p>
      <ul>
        {edges.map((low, index) => {
          const high = thresholds[index];
          const range =
            high === undefined
              ? `≥ ${formatPercent(low, language, 1)}`
              : `${formatNumber(low, language, 1)}–${formatPercent(high, language, 1)}`;
          return (
            <li key={`class-${low}`}>
              <span
                className="legend-swatch legend-rect"
                style={
                  { '--swatch': ramp[Math.min(index, ramp.length - 1)] } as React.CSSProperties
                }
                aria-hidden="true"
              />
              {range}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
