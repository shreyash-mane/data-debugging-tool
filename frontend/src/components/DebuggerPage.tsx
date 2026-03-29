// DebuggerPage.tsx — The main visual debugger: steps, data, diffs, anomalies, charts

import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  AlertCircle, AlertTriangle, CheckCircle2, Loader2,
  ChevronLeft, Download, RefreshCw, Play
} from 'lucide-react';
import {
  getPipeline, getDataset, getRun, listSnapshots, runPipeline
} from '../api/client';
import { useAppStore } from '../store/useAppStore';
import type { StepSnapshot, Anomaly, Explanation, DiffData } from '../types';
import DataTable from './DataTable';
import DiffViewer from './DiffViewer';
import AnomalyCards from './AnomalyCards';
import ExplanationPanel from './ExplanationPanel';
import { RowCountChart, NullCountChart, RowDeltaChart } from './Charts';
import clsx from 'clsx';

type RightTab = 'anomalies' | 'explanations' | 'diff';

export default function DebuggerPage() {
  const { pipelineId, runId } = useParams<{ pipelineId: string; runId: string }>();
  const pid = Number(pipelineId);
  const rid = Number(runId);
  const navigate = useNavigate();

  const {
    snapshots, setSnapshots, activeStepIndex, setActiveStepIndex,
    setActivePipeline, setActiveDataset, setActiveRun, activeRun
  } = useAppStore();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [rerunning, setRerunning] = useState(false);
  const [rightTab, setRightTab] = useState<RightTab>('anomalies');

  useEffect(() => {
    (async () => {
      try {
        const [pipeline, run, snaps] = await Promise.all([
          getPipeline(pid),
          getRun(rid),
          listSnapshots(rid),
        ]);
        setActivePipeline(pipeline);
        setActiveRun(run);
        setSnapshots(snaps);
        setActiveStepIndex(0);

        const dataset = await getDataset(pipeline.dataset_id);
        setActiveDataset(dataset);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [pid, rid]);

  const activeSnapshot: StepSnapshot | null = snapshots[activeStepIndex] ?? null;

  const anomalies: Anomaly[] = activeSnapshot
    ? JSON.parse(activeSnapshot.anomalies_json || '[]')
    : [];

  const explanations: Explanation[] = activeSnapshot
    ? JSON.parse(activeSnapshot.explanation_json || '[]')
    : [];

  const diff: DiffData | null = activeSnapshot
    ? JSON.parse(activeSnapshot.diff_json || '{}')
    : null;

  const sample: Record<string, unknown>[] = activeSnapshot
    ? JSON.parse(activeSnapshot.sample_json || '[]')
    : [];

  const schema: Record<string, string> = activeSnapshot
    ? JSON.parse(activeSnapshot.schema_json || '{}')
    : {};

  const nullCounts: Record<string, number> = activeSnapshot
    ? JSON.parse(activeSnapshot.null_counts_json || '{}')
    : {};

  // Highlight columns that changed in this step
  const changedCols: string[] = diff
    ? [
        ...(diff.columns_added ?? []),
        ...Object.keys(diff.type_changes ?? {}),
      ]
    : [];

  // Compute total anomaly counts per snapshot for sidebar badges
  const anomalyCountPerSnap: Record<number, { critical: number; warning: number }> =
    snapshots.reduce((acc, s) => {
      const anoms: Anomaly[] = JSON.parse(s.anomalies_json || '[]');
      acc[s.step_index] = {
        critical: anoms.filter(a => a.severity === 'critical').length,
        warning: anoms.filter(a => a.severity === 'warning').length,
      };
      return acc;
    }, {} as Record<number, { critical: number; warning: number }>);

  const totalCritical = Object.values(anomalyCountPerSnap).reduce((sum, v) => sum + v.critical, 0);
  const totalWarning = Object.values(anomalyCountPerSnap).reduce((sum, v) => sum + v.warning, 0);

  const handleRerun = async () => {
    setRerunning(true);
    try {
      const run = await runPipeline(pid);
      navigate(`/debug/${pid}/${run.id}`);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setRerunning(false);
    }
  };

  const handleExport = () => {
    const report = {
      pipeline_id: pid,
      run_id: rid,
      run_status: activeRun?.status,
      generated_at: new Date().toISOString(),
      steps: snapshots.map(s => ({
        step: s.step_name,
        row_count: s.row_count,
        col_count: s.col_count,
        anomalies: JSON.parse(s.anomalies_json || '[]'),
        explanations: JSON.parse(s.explanation_json || '[]'),
      })),
    };
    const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `debug_report_run_${rid}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="animate-spin text-accent" size={28} />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center text-red-400 space-y-2">
          <AlertCircle size={32} className="mx-auto" />
          <p className="text-sm">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-[calc(100vh-53px)] overflow-hidden">

      {/* ── LEFT: Step sidebar ─────────────────────────────────────────── */}
      <aside className="w-64 flex-shrink-0 border-r border-surface-3 bg-surface-1 flex flex-col">
        {/* Header */}
        <div className="p-3 border-b border-surface-3 space-y-2">
          <div className="flex items-center gap-1">
            <button
              className="btn-ghost p-1.5 rounded-lg text-xs"
              onClick={() => navigate(`/pipeline/${pid}`)}
            >
              <ChevronLeft size={13} />
            </button>
            <span className="text-xs text-gray-400 font-mono flex-1">Debug Run #{rid}</span>
            <span className={clsx(
              'badge text-[10px]',
              activeRun?.status === 'success' ? 'badge-ok' :
              activeRun?.status === 'failed' ? 'badge-critical' : 'badge-muted'
            )}>
              {activeRun?.status}
            </span>
          </div>

          {/* Summary counts */}
          <div className="flex gap-2 text-[11px]">
            <span className="text-gray-500">{snapshots.length} steps</span>
            {totalCritical > 0 && (
              <span className="text-red-400 flex items-center gap-0.5">
                <AlertCircle size={10} /> {totalCritical}
              </span>
            )}
            {totalWarning > 0 && (
              <span className="text-amber-400 flex items-center gap-0.5">
                <AlertTriangle size={10} /> {totalWarning}
              </span>
            )}
          </div>

          {/* Actions */}
          <div className="flex gap-1.5">
            <button
              className="btn-ghost text-[11px] px-2 py-1.5 flex-1"
              onClick={handleRerun}
              disabled={rerunning}
            >
              {rerunning ? <Loader2 size={11} className="animate-spin" /> : <RefreshCw size={11} />}
              Re-run
            </button>
            <button
              className="btn-ghost text-[11px] px-2 py-1.5 flex-1"
              onClick={handleExport}
            >
              <Download size={11} />
              Export
            </button>
          </div>
        </div>

        {/* Step list */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {snapshots.map((snap, idx) => {
            const counts = anomalyCountPerSnap[snap.step_index];
            const hasCritical = counts?.critical > 0;
            const hasWarning = counts?.warning > 0;
            const isActive = activeStepIndex === idx;

            return (
              <button
                key={snap.id}
                onClick={() => setActiveStepIndex(idx)}
                className={clsx(
                  'w-full text-left transition-all rounded-lg border text-xs',
                  isActive
                    ? 'bg-accent/10 border-accent/30'
                    : hasCritical
                    ? 'bg-red-900/10 border-red-900/30 hover:bg-red-900/15'
                    : hasWarning
                    ? 'bg-amber-900/10 border-amber-900/30 hover:bg-amber-900/15'
                    : 'bg-transparent border-transparent hover:bg-surface-2'
                )}
              >
                <div className="flex items-center gap-2 px-3 py-2.5">
                  {/* Status dot */}
                  <div className={clsx(
                    'w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0 text-[9px] font-mono',
                    hasCritical ? 'bg-red-900/40 text-red-400'
                    : hasWarning ? 'bg-amber-900/40 text-amber-400'
                    : 'bg-ok/20 text-ok'
                  )}>
                    {hasCritical ? '!' : hasWarning ? '~' : '✓'}
                  </div>

                  <div className="flex-1 min-w-0">
                    <p className={clsx(
                      'font-medium truncate',
                      isActive ? 'text-white' : 'text-gray-300'
                    )}>
                      {snap.step_name}
                    </p>
                    <p className="text-gray-500 font-mono text-[10px] mt-0.5">
                      {snap.row_count.toLocaleString()} rows · {snap.col_count} cols
                    </p>
                  </div>

                  {/* Anomaly badges */}
                  <div className="flex flex-col gap-0.5 items-end flex-shrink-0">
                    {hasCritical && (
                      <span className="badge-critical text-[9px] px-1">{counts.critical}</span>
                    )}
                    {hasWarning && (
                      <span className="badge-warning text-[9px] px-1">{counts.warning}</span>
                    )}
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </aside>

      {/* ── CENTER: Data viewer + charts ──────────────────────────────── */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top summary bar */}
        {activeSnapshot && (
          <div className="flex items-center gap-4 px-4 py-2.5 border-b border-surface-3 bg-surface-1/50 flex-shrink-0">
            <div>
              <p className="text-sm font-semibold text-white">{activeSnapshot.step_name}</p>
              <p className="text-[11px] text-gray-500 font-mono">
                Step {activeSnapshot.step_index} of {snapshots.length - 1}
              </p>
            </div>
            <div className="flex gap-4 ml-4">
              <Stat label="Rows" value={activeSnapshot.row_count.toLocaleString()} />
              <Stat label="Cols" value={String(activeSnapshot.col_count)} />
              <Stat label="Nulls" value={
                Object.values(nullCounts).reduce((a, b) => a + b, 0).toLocaleString()
              } />
              <Stat label="Issues" value={String(anomalies.length)}
                className={anomalies.some(a => a.severity === 'critical') ? 'text-red-400' :
                  anomalies.length > 0 ? 'text-amber-400' : 'text-ok'} />
            </div>

            {diff && diff.row_delta !== 0 && diff.row_delta !== undefined && (
              <div className={clsx(
                'ml-auto flex items-center gap-1.5 text-xs font-mono px-2.5 py-1 rounded-lg border',
                diff.row_delta < 0
                  ? 'bg-red-900/20 border-red-800/40 text-red-400'
                  : 'bg-green-900/20 border-green-800/40 text-green-400'
              )}>
                {diff.row_delta > 0 ? '+' : ''}{diff.row_delta.toLocaleString()} rows
                {diff.row_delta_pct !== null && (
                  <span className="opacity-70">
                    ({diff.row_delta > 0 ? '+' : ''}{diff.row_delta_pct?.toFixed(1)}%)
                  </span>
                )}
              </div>
            )}
          </div>
        )}

        {/* Main content area */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {/* Charts row */}
          {snapshots.length > 1 && (
            <div className="grid grid-cols-2 gap-3">
              <RowCountChart snapshots={snapshots} activeIndex={activeStepIndex} />
              <RowDeltaChart snapshots={snapshots} />
            </div>
          )}

          {/* Null chart for current step */}
          {activeSnapshot && (
            <NullCountChart snapshot={activeSnapshot} />
          )}

          {/* Sample data table */}
          {activeSnapshot && (
            <div>
              <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
                Sample Data — {activeSnapshot.step_name}
              </h3>
              <DataTable
                rows={sample}
                schema={schema}
                nullCounts={nullCounts}
                totalRows={activeSnapshot.row_count}
                maxHeight="300px"
                highlightColumns={changedCols}
                caption={changedCols.length > 0
                  ? `Highlighted columns changed at this step: ${changedCols.join(', ')}`
                  : undefined}
              />
            </div>
          )}
        </div>
      </div>

      {/* ── RIGHT: Anomalies / Explanations / Diff ────────────────────── */}
      <aside className="w-80 flex-shrink-0 border-l border-surface-3 bg-surface-1 flex flex-col">
        {/* Tab bar */}
        <div className="flex border-b border-surface-3 flex-shrink-0">
          {(['anomalies', 'explanations', 'diff'] as RightTab[]).map((tab) => (
            <button
              key={tab}
              onClick={() => setRightTab(tab)}
              className={clsx(
                'flex-1 py-2.5 text-xs font-medium transition-colors capitalize relative',
                rightTab === tab
                  ? 'text-white'
                  : 'text-gray-500 hover:text-gray-300'
              )}
            >
              {tab}
              {tab === 'anomalies' && anomalies.length > 0 && (
                <span className={clsx(
                  'ml-1 badge text-[9px] px-1',
                  anomalies.some(a => a.severity === 'critical') ? 'badge-critical' : 'badge-warning'
                )}>
                  {anomalies.length}
                </span>
              )}
              {rightTab === tab && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent" />
              )}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-y-auto p-3">
          {rightTab === 'anomalies' && <AnomalyCards anomalies={anomalies} />}
          {rightTab === 'explanations' && <ExplanationPanel explanations={explanations} />}
          {rightTab === 'diff' && diff && (
            <DiffViewer diff={diff} stepName={activeSnapshot?.step_name ?? ''} />
          )}
          {rightTab === 'diff' && !diff && (
            <p className="text-xs text-gray-600 py-4 text-center">No diff for source step.</p>
          )}
        </div>
      </aside>
    </div>
  );
}

function Stat({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div>
      <p className="text-[10px] text-gray-500 uppercase tracking-wider">{label}</p>
      <p className={clsx('font-mono text-sm font-medium text-white', className)}>{value}</p>
    </div>
  );
}
