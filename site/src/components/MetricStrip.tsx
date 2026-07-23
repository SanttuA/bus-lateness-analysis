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
  const { summary, meta } = overview;
  const metrics = [
    { value: formatNumber(summary.bucket_count, language), label: copy.kpiBuckets },
    { value: formatNumber(summary.line_count, language), label: copy.kpiLines },
    { value: formatNumber(summary.stop_count, language), label: copy.kpiStops },
    { value: formatMinutes(summary.p90_delay_min, language, 2), label: copy.kpiP90 },
    { value: formatPercent(summary.pct_over_5_min_late, language, 2), label: copy.kpiLate },
    { value: formatPercent(meta.conservative_excluded_pct, language, 2), label: copy.kpiExcluded },
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
