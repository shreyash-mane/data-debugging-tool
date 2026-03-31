// AIFixPanel.tsx — Claude-powered root cause analysis with one-click fix application

import { useState, useCallback } from 'react';
import {
  Sparkles, Loader2, AlertCircle, AlertTriangle, Info,
  CheckCircle2, ChevronDown, ChevronUp, Wrench, Zap,
} from 'lucide-react';
import { getAIExplanation, createStep } from '../api/client';
import type { AIExplanation, StepType } from '../types';
import clsx from 'clsx';

interface Props {
  snapshotId: number | null;
  pipelineId: number;
  currentStepCount: number;
}

const SEVERITY_STYLES = {
  critical: {
    badge: 'bg-red-900/30 text-red-400 border-red-800/40',
    border: 'border-red-900/30',
    icon: AlertCircle,
    iconClass: 'text-red-400',
  },
  warning: {
    badge: 'bg-amber-900/30 text-amber-400 border-amber-800/40',
    border: 'border-amber-900/30',
    icon: AlertTriangle,
    iconClass: 'text-amber-400',
  },
  info: {
    badge: 'bg-blue-900/30 text-blue-400 border-blue-800/40',
    border: 'border-blue-900/30',
    icon: Info,
    iconClass: 'text-blue-400',
  },
};

const STEP_TYPE_LABELS: Record<string, string> = {
  auto_clean: 'Auto Clean',
  filter_rows: 'Filter Rows',
  fill_missing: 'Fill Missing',
  change_dtype: 'Change Type',
  remove_duplicates: 'Remove Duplicates',
  rename_column: 'Rename Column',
  drop_missing: 'Drop Missing',
};

function ConfigPreview({ config }: { config: Record<string, any> }) {
  const entries = Object.entries(config);
  if (entries.length === 0) return <span className="text-gray-500 italic">no config (auto)</span>;
  return (
    <span className="font-mono text-[10px] text-gray-400">
      {entries.map(([k, v]) => `${k}: ${JSON.stringify(v)}`).join(', ')}
    </span>
  );
}

function IssueCard({
  item,
  index,
  pipelineId,
  stepOrder,
}: {
  item: AIExplanation;
  index: number;
  pipelineId: number;
  stepOrder: number;
}) {
  const [expanded, setExpanded] = useState(index === 0);
  const [applying, setApplying] = useState(false);
  const [applied, setApplied] = useState(false);
  const [applyError, setApplyError] = useState<string | null>(null);

  const styles = SEVERITY_STYLES[item.severity] ?? SEVERITY_STYLES.info;
  const SeverityIcon = styles.icon;

  const handleApply = async () => {
    setApplying(true);
    setApplyError(null);
    try {
      await createStep(pipelineId, {
        name: item.issue,
        step_type: item.suggested_step_type as StepType,
        config_json: JSON.stringify(item.suggested_config),
        order: stepOrder + index,
        enabled: true,
      });
      setApplied(true);
    } catch (e: any) {
      setApplyError(e.message);
    } finally {
      setApplying(false);
    }
  };

  return (
    <div className={clsx('rounded-lg border overflow-hidden', styles.border, 'bg-surface-2/60')}>
      {/* Header */}
      <div
        className="px-3 py-2.5 flex items-start gap-2 cursor-pointer"
        onClick={() => setExpanded(e => !e)}
      >
        <SeverityIcon size={13} className={clsx('flex-shrink-0 mt-0.5', styles.iconClass)} />
        <div className="flex-1 min-w-0">
          <p className="text-xs font-medium text-gray-200 leading-snug">{item.issue}</p>
          {item.example_values?.length > 0 && (
            <p className="text-[10px] text-gray-500 font-mono mt-0.5 truncate">
              e.g. {item.example_values.slice(0, 3).join(', ')}
            </p>
          )}
        </div>
        <div className="flex items-center gap-1.5 flex-shrink-0">
          <span className={clsx('text-[9px] px-1.5 py-0.5 rounded border font-semibold uppercase', styles.badge)}>
            {item.severity}
          </span>
          {expanded
            ? <ChevronUp size={11} className="text-gray-600" />
            : <ChevronDown size={11} className="text-gray-600" />}
        </div>
      </div>

      {/* Expanded body */}
      {expanded && (
        <div className="border-t border-surface-3 px-3 py-3 space-y-3 text-xs">
          {/* Root cause */}
          <div>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">Root Cause</p>
            <p className="text-gray-300 leading-relaxed">{item.root_cause}</p>
          </div>

          {/* Explanation */}
          <div>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">Fix Explanation</p>
            <p className="text-gray-300 leading-relaxed">{item.explanation}</p>
          </div>

          {/* Suggested step */}
          <div className="bg-surface-3/50 rounded-lg p-2.5 space-y-1.5">
            <p className="text-[10px] text-gray-500 uppercase tracking-wider flex items-center gap-1">
              <Wrench size={10} /> Suggested Step
            </p>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-[11px] font-semibold text-accent">
                {STEP_TYPE_LABELS[item.suggested_step_type] ?? item.suggested_step_type}
              </span>
              <ConfigPreview config={item.suggested_config ?? {}} />
            </div>
          </div>

          {/* Apply button */}
          {applied ? (
            <div className="flex items-center gap-1.5 text-ok text-[11px]">
              <CheckCircle2 size={13} />
              Step added to pipeline — re-run to see results
            </div>
          ) : (
            <div className="space-y-1.5">
              <button
                onClick={handleApply}
                disabled={applying}
                className={clsx(
                  'w-full flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium transition-all',
                  applying
                    ? 'bg-surface-3 text-gray-500 cursor-not-allowed'
                    : 'bg-accent/15 hover:bg-accent/25 text-accent border border-accent/30 hover:border-accent/50'
                )}
              >
                {applying
                  ? <><Loader2 size={12} className="animate-spin" /> Applying…</>
                  : <><Zap size={12} /> Apply Fix</>}
              </button>
              {applyError && (
                <p className="text-[10px] text-red-400">{applyError}</p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}


export default function AIFixPanel({ snapshotId, pipelineId, currentStepCount }: Props) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{ explanations: AIExplanation[]; model: string } | null>(null);
  const [analysed, setAnalysed] = useState<number | null>(null);

  const runAnalysis = useCallback(async () => {
    if (!snapshotId) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await getAIExplanation(snapshotId);
      setResult({ explanations: data.explanations, model: data.model });
      setAnalysed(snapshotId);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [snapshotId]);

  // Reset when snapshot changes
  if (analysed !== null && analysed !== snapshotId) {
    setResult(null);
    setAnalysed(null);
    setError(null);
  }

  if (!snapshotId) {
    return (
      <div className="text-center py-8 text-gray-600 text-xs">
        Select a step to run AI analysis.
      </div>
    );
  }

  if (!result && !loading) {
    return (
      <div className="flex flex-col items-center gap-4 py-8 px-3 text-center">
        <div className="w-12 h-12 rounded-full bg-accent/10 border border-accent/20 flex items-center justify-center">
          <Sparkles size={22} className="text-accent" />
        </div>
        <div className="space-y-1">
          <p className="text-sm font-semibold text-white">AI Root Cause Analysis</p>
          <p className="text-[11px] text-gray-500 leading-relaxed max-w-xs">
            Claude claude-sonnet-4-6 will inspect the sample data, detect specific issues,
            explain each root cause, and suggest exact pipeline steps to fix them.
          </p>
        </div>
        <button
          onClick={runAnalysis}
          className="btn-primary flex items-center gap-1.5"
        >
          <Sparkles size={14} />
          Analyse with AI
        </button>
        {error && (
          <div className="w-full p-3 bg-red-900/20 border border-red-800/40 rounded-lg text-xs text-red-400 text-left">
            <p className="font-medium mb-1">Analysis failed</p>
            <p className="leading-relaxed">{error}</p>
            {error.includes('ANTHROPIC_API_KEY') && (
              <p className="mt-2 text-amber-400">
                Add ANTHROPIC_API_KEY to your Railway environment variables and redeploy.
              </p>
            )}
          </div>
        )}
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center gap-3 py-8 text-center">
        <Loader2 size={24} className="animate-spin text-accent" />
        <p className="text-xs text-gray-400">Claude is analysing your data…</p>
        <p className="text-[10px] text-gray-600">This usually takes 5–15 seconds</p>
      </div>
    );
  }

  if (result) {
    const { explanations, model } = result;
    return (
      <div className="space-y-3">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5 text-[10px] text-ok">
            <Sparkles size={11} />
            <span className="font-medium">{model}</span>
          </div>
          <button
            onClick={runAnalysis}
            className="text-[10px] text-gray-500 hover:text-gray-300 transition-colors"
          >
            Re-analyse
          </button>
        </div>

        {explanations.length === 0 ? (
          <div className="text-center py-6 space-y-2">
            <CheckCircle2 size={24} className="mx-auto text-ok" />
            <p className="text-xs text-gray-300 font-medium">No issues found</p>
            <p className="text-[10px] text-gray-600">Claude found no data quality problems at this step.</p>
          </div>
        ) : (
          <>
            <p className="text-[10px] text-gray-500">
              {explanations.length} issue{explanations.length !== 1 ? 's' : ''} found —
              click <strong className="text-white">Apply Fix</strong> to add the step to your pipeline
            </p>
            <div className="space-y-2">
              {explanations.map((item, i) => (
                <IssueCard
                  key={i}
                  item={item}
                  index={i}
                  pipelineId={pipelineId}
                  stepOrder={currentStepCount}
                />
              ))}
            </div>
          </>
        )}
      </div>
    );
  }

  return null;
}
