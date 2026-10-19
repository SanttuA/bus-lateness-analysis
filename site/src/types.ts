export type Language = 'fi' | 'en';
export type DayFilter = 'all' | 'weekday' | 'weekend';
export type DayType = Exclude<DayFilter, 'all'>;
export type DelayDirection = 'late' | 'early';
export type ExplorerView = 'map' | 'table';
export type LineScope = 'frequent' | 'all';

export interface SearchState {
  line?: string;
  direction?: '1' | '2';
  day?: DayType;
  from?: number;
  to?: number;
  stop?: string;
  metric?: DelayDirection;
  rank?: DelayDirection;
  heat?: DelayDirection;
  view?: ExplorerView;
  lines?: LineScope;
}

/** Shares are percentages of trip–stop buckets ("stop passes"); delays are minutes. */
export interface ShareMetrics {
  bucket_count: number;
  median_delay_min: number;
  p90_delay_min: number;
  pct_on_time: number;
  pct_over_5_min_late: number;
  pct_over_3_min_early: number;
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
  minimum_trip_count: number;
  frequent_trips_per_day: number;
  on_time_band_min: [number, number];
  holiday_dates: string[];
  conservative_excluded_pct: number;
  placeholder_rows_excluded: number;
  license: string;
}

export interface Summary extends ShareMetrics {
  raw_poll_count: number;
  trip_count: number;
  day_count: number;
  p75_delay_min: number;
  p95_delay_min: number;
  pct_late: number;
  pct_over_3_min_late: number;
  pct_early: number;
  pct_over_1_min_early: number;
  line_count: number;
  stop_count: number;
  start_date: string;
  end_date: string;
}

export interface Takeaway {
  id: string;
  fi: string;
  en: string;
}

export type Caveat = Takeaway;

export interface Headlines {
  ranking: string;
  hourly: string;
  rush: string;
  alerts: string;
  daily: string;
  recommendations: string[];
}

export interface HourMetric extends ShareMetrics {
  day_type: DayType;
  local_hour: number;
  trip_count: number;
  p75_delay_min: number;
}

export interface OverviewPayload {
  schema_version: 2;
  meta: SnapshotMeta;
  summary: Summary;
  headlines: Record<Language, Headlines>;
  takeaways: Takeaway[];
  caveats: Caveat[];
  hourly_profile: HourMetric[];
}

export interface LineMetric extends ShareMetrics {
  line_ref: string;
  line_name: string;
  trip_count: number;
  day_count: number;
  trips_per_day: number;
  p90_delay_ci_lo: number | null;
  p90_delay_ci_hi: number | null;
  pct_over_3_min_late: number;
  pct_over_5_min_late_ci_lo: number | null;
  pct_over_5_min_late_ci_hi: number | null;
  pct_over_1_min_early: number;
  pct_over_3_min_early_ci_lo: number | null;
  pct_over_3_min_early_ci_hi: number | null;
  excluded_pct: number | null;
  data_fault_pct: number | null;
  trip_coverage_pct: number | null;
  low_quality: boolean;
  direction_labels: Record<string, string>;
}

export interface LineHourMetric extends ShareMetrics {
  line_ref: string;
  direction_ref: string;
  day_type: DayType;
  local_hour: number;
  trip_count: number;
}

export interface LinesPayload {
  schema_version: 2;
  lines: LineMetric[];
  contexts: LineHourMetric[];
}

export interface StopMetric extends ShareMetrics {
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
  schema_version: 2;
  stops: StopRecord[];
  metrics: StopMetric[];
}

export interface RushImpact {
  line_ref: string;
  line_name: string;
  bucket_count_offpeak: number;
  bucket_count_rush: number;
  trip_count_offpeak: number;
  trip_count_rush: number;
  median_delay_min_offpeak: number;
  median_delay_min_rush: number;
  p90_delay_min_offpeak: number;
  p90_delay_min_rush: number;
  rush_p90_delay_lift_min: number;
  pct_over_5_min_late_offpeak: number;
  pct_over_5_min_late_rush: number;
  rush_over_5_min_late_pct_point_lift: number;
  rush_over_5_min_late_lift_ci_lo: number | null;
  rush_over_5_min_late_lift_ci_hi: number | null;
}

export interface AlertLift {
  cause: string;
  effect: string;
  priority: number;
  alert_scope: string;
  alert_count: number;
  active_trip_count: number;
  bucket_count_alert: number;
  bucket_count_control: number;
  coverage_pct: number;
  pct_over_5_min_late_alert: number;
  pct_over_5_min_late_control: number;
  late5_lift_pp: number;
  late5_lift_lo: number | null;
  late5_lift_hi: number | null;
  median_lift_min: number;
}

export interface AlertInventory {
  included: number;
  long_running: number;
  no_targets: number;
  never_active: number;
}

export interface CancellationSummary {
  total_departures: number;
  by_line: { line_ref: string; cancelled_departures: number }[];
  by_cause: { cause: string; cancelled_departures: number }[];
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

export interface DailyMetric extends ShareMetrics {
  local_date: string;
  local_weekday: number;
  day_type: DayType;
  is_holiday: boolean;
  trip_count: number;
  collector_coverage_pct: number | null;
  collector_missing_min: number | null;
}

export interface ContextPayload {
  schema_version: 2;
  rush_impact: RushImpact[];
  alerts: AlertLift[];
  alert_inventory: AlertInventory;
  cancellations: CancellationSummary;
  quality: QualityMetric[];
  collector_gaps: CollectorGap[];
  daily: DailyMetric[];
}

export interface StopMapPoint extends StopRecord, StopMetric {
  display_value: number;
}
