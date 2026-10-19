import { formatMinutes, formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import type { Language, OverviewPayload } from '../types';

export function MetricStrip({
  language,
  overview,
}: {
  language: Language;
  overview: OverviewPayload;
}) {
  const copy = t(language);
  const { summary } = overview;
  const metrics = [
    { value: formatPercent(summary.pct_on_time, language, 1), label: copy.kpiOnTime },
    { value: formatPercent(summary.pct_over_5_min_late, language, 1), label: copy.kpiLate },
    { value: formatPercent(summary.pct_over_3_min_early, language, 1), label: copy.kpiEarly },
    { value: formatMinutes(summary.p90_delay_min, language, 1), label: copy.kpiP90 },
    { value: formatNumber(summary.trip_count, language), label: copy.kpiTrips },
    { value: formatNumber(summary.day_count, language), label: copy.kpiDays },
  ];
  return (
    <dl className="metric-strip">
      {metrics.map((metric) => (
        <div key={metric.label}>
          <dt>{metric.label}</dt>
          <dd>{metric.value}</dd>
        </div>
      ))}
    </dl>
  );
}
