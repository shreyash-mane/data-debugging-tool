// PipelineBuilder.tsx — Full pipeline step builder with CRUD

import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Plus, Play, Trash2, GripVertical, ChevronDown, ChevronUp,
  AlertCircle, Loader2, ToggleLeft, ToggleRight, Edit3, ArrowLeft
} from 'lucide-react';
import {
  getPipeline, getDataset, listSteps, createStep, updateStep,
  deleteStep, reorderSteps, runPipeline, listUploads,
} from '../api/client';
import { useAppStore } from '../store/useAppStore';
import type { PipelineStep, StepType } from '../types';
import StepEditor from './StepEditor';
import clsx from 'clsx';

const STEP_TYPE_LABEL: Record<string, string> = {
  auto_clean: 'Auto Clean',
  drop_missing: 'Drop Missing',
  fill_missing: 'Fill Missing',
  rename_column: 'Rename Column',
  change_dtype: 'Change Type',
  filter_rows: 'Filter Rows',
  select_columns: 'Select Columns',
  sort_values: 'Sort Values',
  remove_duplicates: 'Dedup',
  add_computed_column: 'Computed Col',
  join: 'Join',
  group_aggregate: 'Group Agg',
};

export default function PipelineBuilder() {
  const { pipelineId } = useParams<{ pipelineId: string }>();
  const pid = Number(pipelineId);
  const navigate = useNavigate();

  const {
    steps, setSteps, upsertStep, removeStep,
    setActivePipeline, setActiveDataset, setActiveRun,
    activePipeline, activeDataset,
  } = useAppStore();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showEditor, setShowEditor] = useState(false);
  const [editingStep, setEditingStep] = useState<PipelineStep | null>(null);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [uploadedFiles, setUploadedFiles] = useState<string[]>([]);
  const [expandedStep, setExpandedStep] = useState<number | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const pipeline = await getPipeline(pid);
        setActivePipeline(pipeline);
        const dataset = await getDataset(pipeline.dataset_id);
        setActiveDataset(dataset);
        const stps = await listSteps(pid);
        setSteps(stps);
        const { files } = await listUploads();
        setUploadedFiles(files);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [pid]);

  // Derive the schema columns from dataset
  const columns: string[] = activeDataset
    ? Object.keys(JSON.parse(activeDataset.schema_json))
    : [];

  const handleSave = async (data: {
    name: string; step_type: StepType; config_json: string; order: number;
  }) => {
    setSaving(true);
    try {
      if (editingStep) {
        const updated = await updateStep(editingStep.id, data);
        upsertStep(updated);
      } else {
        const created = await createStep(pid, {
          ...data,
          order: steps.length,
        });
        upsertStep(created);
      }
      setShowEditor(false);
      setEditingStep(null);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (step: PipelineStep) => {
    if (!confirm(`Delete step "${step.name}"?`)) return;
    await deleteStep(step.id);
    removeStep(step.id);
  };

  const handleToggle = async (step: PipelineStep) => {
    const updated = await updateStep(step.id, { enabled: !step.enabled });
    upsertStep(updated);
  };

  const moveStep = async (step: PipelineStep, direction: -1 | 1) => {
    const sorted = [...steps];
    const idx = sorted.findIndex(s => s.id === step.id);
    const newIdx = idx + direction;
    if (newIdx < 0 || newIdx >= sorted.length) return;

    // Swap
    const orderMap = sorted.map((s, i) => ({ step_id: s.id, order: i }));
    orderMap[idx].order = newIdx;
    orderMap[newIdx].order = idx;

    await reorderSteps(pid, orderMap);
    const updated = [...steps];
    updated[idx] = { ...step, order: newIdx };
    updated[newIdx] = { ...sorted[newIdx], order: idx };
    setSteps(updated.sort((a, b) => a.order - b.order));
  };

  const handleRun = async () => {
    setError(null);
    setRunning(true);
    try {
      const run = await runPipeline(pid);
      setActiveRun(run);
      navigate(`/debug/${pid}/${run.id}`);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setRunning(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="animate-spin text-accent" size={28} />
      </div>
    );
  }

  return (
    <div className="flex h-[calc(100vh-53px)] overflow-hidden">
      {/* ── Left: step list ───────────────────────────────────────────── */}
      <aside className="w-80 flex-shrink-0 border-r border-surface-3 bg-surface-1 flex flex-col">
        {/* Header */}
        <div className="p-4 border-b border-surface-3 space-y-3">
          <div className="flex items-center gap-2">
            <button
              onClick={() => navigate('/upload')}
              className="btn-ghost p-1.5 rounded-lg"
            >
              <ArrowLeft size={14} />
            </button>
            <div className="flex-1 min-w-0">
              <h2 className="text-sm font-semibold text-white truncate">
                {activePipeline?.name}
              </h2>
              <p className="text-[11px] text-gray-500">
                {steps.length} step{steps.length !== 1 ? 's' : ''} · {activeDataset?.name}
              </p>
            </div>
          </div>

          <div className="flex gap-2">
            <button
              className="btn-primary flex-1 text-xs"
              onClick={handleRun}
              disabled={running || steps.filter(s => s.enabled).length === 0}
            >
              {running ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />}
              {running ? 'Running…' : 'Run Pipeline'}
            </button>
            <button
              className="btn-ghost text-xs px-3"
              onClick={() => { setEditingStep(null); setShowEditor(true); }}
            >
              <Plus size={13} />
              Add
            </button>
          </div>
        </div>

        {error && (
          <div className="mx-4 mt-3 p-2.5 bg-red-900/20 border border-red-800/40 rounded-lg flex items-start gap-2 text-xs text-red-400">
            <AlertCircle size={12} className="mt-0.5 flex-shrink-0" />
            {error}
          </div>
        )}

        {/* Step list */}
        <div className="flex-1 overflow-y-auto p-3 space-y-1.5">
          {steps.length === 0 && (
            <div className="text-center py-10 text-gray-600 text-xs space-y-2">
              <p>No steps yet.</p>
              <p>Click "Add" to start building your pipeline.</p>
            </div>
          )}

          {/* Source pseudo-step */}
          {steps.length > 0 && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-surface-2/50 border border-surface-3 text-xs text-gray-500">
              <div className="w-5 h-5 rounded-full bg-ok/20 flex items-center justify-center flex-shrink-0">
                <span className="text-ok text-[9px] font-bold">S</span>
              </div>
              <span>Source Dataset</span>
              <span className="ml-auto font-mono text-[10px]">
                {activeDataset?.row_count.toLocaleString()} rows
              </span>
            </div>
          )}

          {steps.map((step, idx) => (
            <div key={step.id}>
              {/* Connector */}
              <div className="ml-4 w-px h-2 bg-surface-4" />

              <div
                className={clsx(
                  'rounded-lg border text-xs transition-all',
                  !step.enabled && 'opacity-50',
                  expandedStep === step.id
                    ? 'bg-accent/5 border-accent/30'
                    : 'bg-surface-2 border-surface-4'
                )}
              >
                {/* Step header */}
                <div
                  className="flex items-center gap-2 px-3 py-2.5 cursor-pointer"
                  onClick={() => setExpandedStep(expandedStep === step.id ? null : step.id)}
                >
                  <div className="w-5 h-5 rounded-full bg-surface-3 flex items-center justify-center flex-shrink-0">
                    <span className="text-gray-400 text-[9px] font-mono">{idx + 1}</span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-gray-200 truncate">{step.name}</p>
                    <p className="text-[10px] text-gray-500 font-mono mt-0.5">
                      {STEP_TYPE_LABEL[step.step_type] ?? step.step_type}
                    </p>
                  </div>
                  {expandedStep === step.id ? (
                    <ChevronUp size={12} className="text-gray-500 flex-shrink-0" />
                  ) : (
                    <ChevronDown size={12} className="text-gray-500 flex-shrink-0" />
                  )}
                </div>

                {/* Step actions (expanded) */}
                {expandedStep === step.id && (
                  <div className="border-t border-surface-4 px-3 py-2 flex items-center gap-1">
                    <button
                      className="btn-ghost p-1.5 text-[11px] gap-1"
                      onClick={() => { setEditingStep(step); setShowEditor(true); setExpandedStep(null); }}
                      title="Edit"
                    >
                      <Edit3 size={12} /> Edit
                    </button>
                    <button
                      className="btn-ghost p-1.5 text-[11px] gap-1"
                      onClick={() => handleToggle(step)}
                      title={step.enabled ? 'Disable' : 'Enable'}
                    >
                      {step.enabled ? <ToggleRight size={12} className="text-ok" /> : <ToggleLeft size={12} />}
                      {step.enabled ? 'Enabled' : 'Disabled'}
                    </button>
                    <div className="flex gap-0.5 ml-auto">
                      <button
                        className="btn-ghost p-1"
                        onClick={() => moveStep(step, -1)}
                        disabled={idx === 0}
                      >
                        <ChevronUp size={12} />
                      </button>
                      <button
                        className="btn-ghost p-1"
                        onClick={() => moveStep(step, 1)}
                        disabled={idx === steps.length - 1}
                      >
                        <ChevronDown size={12} />
                      </button>
                      <button
                        className="btn-danger p-1"
                        onClick={() => handleDelete(step)}
                      >
                        <Trash2 size={12} />
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </aside>

      {/* ── Right: editor panel ───────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-6">
        {showEditor ? (
          <div className="max-w-xl mx-auto">
            <h2 className="text-base font-semibold text-white mb-4">
              {editingStep ? `Edit: ${editingStep.name}` : 'Add Step'}
            </h2>
            <div className="card p-5">
              <StepEditor
                columns={columns}
                uploadedFiles={uploadedFiles}
                step={editingStep}
                nextOrder={steps.length}
                onSave={handleSave}
                onCancel={() => { setShowEditor(false); setEditingStep(null); }}
                saving={saving}
              />
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-center gap-4">
            {steps.length === 0 ? (
              <>
                <div className="text-gray-600 text-sm max-w-sm">
                  <p className="text-lg mb-2 text-white">Build your pipeline</p>
                  <p>Add transformation steps to clean and reshape your data.</p>
                  <p className="mt-1 text-xs">Then click <strong className="text-white">Run Pipeline</strong> to debug it step by step.</p>
                </div>
                <div className="p-4 bg-ok/5 border border-ok/25 rounded-xl max-w-sm w-full text-left space-y-2">
                  <p className="text-xs font-semibold text-ok">Recommended first step</p>
                  <p className="text-[11px] text-gray-400">
                    <strong className="text-white">Auto Clean</strong> detects column types, strips symbols,
                    and fills all missing values automatically — no configuration needed.
                  </p>
                  <button
                    className="btn-primary w-full text-xs"
                    onClick={() => {
                      setEditingStep(null);
                      setShowEditor(true);
                    }}
                  >
                    <Plus size={13} /> Add Auto Clean Step
                  </button>
                </div>
              </>
            ) : (
              <div className="text-gray-600 text-sm space-y-2">
                <p className="text-white font-medium">{steps.filter(s => s.enabled).length} enabled step{steps.filter(s => s.enabled).length !== 1 ? 's' : ''} ready</p>
                <p>Click a step to edit, or run the pipeline to start debugging.</p>
                <button
                  className="btn-primary mt-3"
                  onClick={handleRun}
                  disabled={running}
                >
                  {running ? <Loader2 size={15} className="animate-spin" /> : <Play size={15} />}
                  {running ? 'Running…' : 'Run Pipeline'}
                </button>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
