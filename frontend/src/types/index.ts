// types/index.ts — Shared TypeScript types mirroring backend Pydantic schemas

export interface Dataset {
  id: number;
  name: string;
  filename: string;
  row_count: number;
  col_count: number;
  schema_json: string;
  stats_json: string;
  sample_json: string;
  created_at: string;
}

export interface Pipeline {
  id: number;
  name: string;
  dataset_id: number;
  created_at: string;
  updated_at: string;
}

export interface PipelineStep {
  id: number;
  pipeline_id: number;
  name: string;
  step_type: StepType;
  config_json: string;
  order: number;
  enabled: boolean;
}

export type StepType =
  | 'auto_clean'
  | 'drop_missing'
  | 'fill_missing'
  | 'rename_column'
  | 'change_dtype'
  | 'filter_rows'
  | 'select_columns'
  | 'sort_values'
  | 'remove_duplicates'
  | 'add_computed_column'
  | 'join'
  | 'group_aggregate';

export interface PipelineRun {
  id: number;
  pipeline_id: number;
  status: 'pending' | 'running' | 'success' | 'failed';
  error_message: string | null;
  started_at: string;
  finished_at: string | null;
}

export interface StepSnapshot {
  id: number;
  run_id: number;
  step_id: number | null;
  step_index: number;
  step_name: string;
  row_count: number;
  col_count: number;
  schema_json: string;
  stats_json: string;
  null_counts_json: string;
  sample_json: string;
  diff_json: string;
  anomalies_json: string;
  explanation_json: string;
  created_at: string;
}

// ── Parsed diff structure ─────────────────────────────────────────────────────

export interface DiffData {
  row_count_before: number;
  row_count_after: number;
  row_delta: number;
  row_delta_pct: number | null;
  col_count_before: number;
  col_count_after: number;
  columns_added: string[];
  columns_removed: string[];
  type_changes: Record<string, { before: string; after: string }>;
  null_changes: Record<string, {
    before: number | null;
    after: number;
    delta: number | null;
    delta_pct: number | null;
  }>;
  duplicate_before: number;
  duplicate_after: number;
  duplicate_delta: number;
  stat_drift: Record<string, {
    mean_before: number | null;
    mean_after: number | null;
    drift_pct: number | null;
    min_before: number | null;
    min_after: number | null;
    max_before: number | null;
    max_after: number | null;
  }>;
  category_shifts: Record<string, {
    added_values: string[];
    removed_values: string[];
  }>;
  distribution_shift: Record<string, {
    ks_stat: number;
    ks_pvalue: number;
  }>;
}

// ── Anomaly ───────────────────────────────────────────────────────────────────

export interface Anomaly {
  type: string;
  severity: 'critical' | 'warning' | 'info';
  column: string | null;
  message: string;
  value: number | string | null;
  threshold: number | null;
}

// ── Explanation ───────────────────────────────────────────────────────────────

export interface AutoCleanImputation {
  decision: string;
  method: string;
  reason: string;
  missing_count?: number;
  missing_pct?: number;
  skewness?: number;
  fill_value?: number | string;
  unique_count?: number;
}

export interface AutoCleanIssue {
  type: string;
  severity: 'critical' | 'warning' | 'info';
  message: string;
  count: number;
}

export interface Explanation {
  anomaly_type: string;
  severity: 'critical' | 'warning' | 'info';
  column: string | null;
  raw_message: string;
  summary: string;
  likely_cause: string;
  confidence: 'high' | 'medium' | 'low';
  recommended_checks: string[];
  suggested_fix: string;
  // Auto-clean enrichment (only present when anomaly_type === 'auto_clean')
  detected_type?: string;
  cleaning_steps?: string[];
  issues_found?: AutoCleanIssue[];
  imputation?: AutoCleanImputation;
}

// ── AI Explanation ────────────────────────────────────────────────────────────

export interface AIExplanation {
  issue: string;
  root_cause: string;
  severity: 'critical' | 'warning' | 'info';
  example_values: string[];
  suggested_step_type: string;
  suggested_config: Record<string, any>;
  explanation: string;
}

export interface AIExplainResponse {
  snapshot_id: number;
  step_name: string;
  explanations: AIExplanation[];
  model: string;
}

// ── Parsed schema/stats ────────────────────────────────────────────────────────

export interface ColumnStats {
  dtype: string;
  null_count: number;
  unique_count: number;
  min?: number;
  max?: number;
  mean?: number;
  std?: number;
  median?: number;
  top_values?: Record<string, number>;
}

export type SchemaMap = Record<string, string>;
export type StatsMap = Record<string, ColumnStats>;
export type NullCountMap = Record<string, number>;
