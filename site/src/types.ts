export type Language = 'fi' | 'en';
export type DayFilter = 'all' | 'weekday' | 'weekend';
export type DelayDirection = 'late' | 'early';
export type ExplorerView = 'map' | 'table';

export interface SearchState {
  line?: string;
  direction?: '1' | '2';
  day?: Exclude<DayFilter, 'all'>;
  from?: number;
  to?: number;
  stop?: string;
  metric?: DelayDirection;
  view?: ExplorerView;
}

export interface MetricBase {
  bucket_count: number;
  raw_poll_count: number;
  signed_mean_delay_min: number;
  median_delay_min: number;
  p90_delay_min: number;
  pct_over_5_min_late: number;
  pct_over_3_min_early: number;
  p90_early_min_abs: number;
}

export interface SnapshotMeta {
  title_fi: string;
  title_en: string;
  generated_at_utc: string;
  analysis_start_utc: string;
  analysis_end_utc: string;
  timezone: string;
  bucket_mode: string;
  quality_mode: string;
  minimum_bucket_count: number;
  conservative_excluded_pct: number;
  license: string;
}

export interface Summary extends MetricBase {
  line_count: number;
  stop_count: number;
  start_date: string;
  end_date: string;
  pct_early: number;
  pct_late: number;
  pct_over_3_min_late: number;
  pct_over_1_min_early: number;
  p75_delay_min: number;
  p95_delay_min: number;
}

export interface Takeaway {
  id: string;
  fi: string;
  en: string;
}

export type Caveat = Takeaway;

export interface HourMetric extends MetricBase {
  local_hour: number;
  pct_early: number;
  pct_over_3_min_late: number;
  pct_over_1_min_early: number;
  p75_delay_min: number;
  p95_delay_min: number;
}

export interface OverviewPayload {
  schema_version: 1;
  meta: SnapshotMeta;
  summary: Summary;
  takeaways: Takeaway[];
  caveats: Caveat[];
  hourly_profile: HourMetric[];
}

export interface LineMetric extends MetricBase {
  line_ref: string;
  line_name: string;
  pct_early: number;
  pct_over_3_min_late: number;
  pct_over_1_min_early: number;
  p75_delay_min: number;
  p95_delay_min: number;
}

export interface LineHourMetric extends MetricBase {
  line_ref: string;
  line_name: string;
  direction_ref: string;
  day_type: 'weekday' | 'weekend';
  local_hour: number;
  pct_early: number;
  pct_over_3_min_late: number;
  pct_over_1_min_early: number;
  p75_delay_min: number;
  p95_delay_min: number;
}

export interface LinesPayload {
  schema_version: 1;
  lines: LineMetric[];
  contexts: LineHourMetric[];
}

export interface StopMetric extends MetricBase {
  stop_id: string;
  line_ref: string;
  day_type: DayFilter;
}

export interface StopRecord {
  stop_id: string;
  stop_name: string;
  stop_lat: number | null;
  stop_lon: number | null;
  line_count: number;
}

export interface StopsPayload {
  schema_version: 1;
  stops: StopRecord[];
  metrics: StopMetric[];
}

export interface RushImpact {
  line_ref: string;
  line_name: string;
  bucket_count_non_rush: number;
  bucket_count_rush: number;
  p90_delay_min_non_rush: number;
  p90_delay_min_rush: number;
  rush_p90_delay_lift_min: number;
  rush_over_5_min_late_pct_point_lift: number;
}

export interface AlertLift {
  cause: string;
  effect: string;
  priority: number;
  alert_scope: string;
  bucket_count_control: number;
  bucket_count_alert: number;
  median_delay_lift_min: number;
  p90_delay_lift_min: number;
  over_5_min_late_pct_point_lift: number;
}

export interface QualityMetric {
  quality_check: string;
  row_count: number;
  pct_rows: number;
}

export interface CollectorGap {
  source: string;
  gap_start_utc: string;
  gap_end_utc: string;
  gap_min: number;
  missing_min: number;
  estimated_missed_polls: number;
}

export interface StopChange {
  stop_id: string;
  stop_name: string;
  baseline_bucket_count: number;
  comparison_bucket_count: number;
  median_delay_change_min: number;
  p90_delay_change_min: number;
  over_5_min_late_pct_point_change: number;
}

export interface ContextPayload {
  schema_version: 1;
  rush_impact: RushImpact[];
  alerts: AlertLift[];
  quality: QualityMetric[];
  collector_gaps: CollectorGap[];
  stop_changes: StopChange[];
}

export interface StopMapPoint extends StopRecord, StopMetric {
  display_value: number;
}
