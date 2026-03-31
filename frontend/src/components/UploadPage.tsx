import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Upload, Trash2, Table, Plus, ChevronRight,
  FileText, AlertCircle, Loader2
} from 'lucide-react';
import { uploadDataset, listDatasets, deleteDataset, createPipeline } from '../api/client';
import { useAppStore } from '../store/useAppStore';
import type { Dataset } from '../types';
import DataTable from './DataTable';
import clsx from 'clsx';

export default function UploadPage() {
  const navigate = useNavigate();
  const { datasets, setDatasets, setActiveDataset, setActivePipeline, activePipeline } = useAppStore();
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<Dataset | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [newPipelineName, setNewPipelineName] = useState('');
  const [creatingPipeline, setCreatingPipeline] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    listDatasets().then(setDatasets).catch(console.error);
  }, []);

  const handleFile = async (file: File) => {
    if (!file.name.endsWith('.csv')) {
      setError('Only .csv files are supported.');
      return;
    }
    setError(null);
    setUploading(true);
    try {
      const ds = await uploadDataset(file);
      setDatasets([ds, ...datasets]);
      setPreview(ds);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  const handleDelete = async (ds: Dataset, e: React.MouseEvent) => {
    e.stopPropagation();
    await deleteDataset(ds.id);
    setDatasets(datasets.filter((d) => d.id !== ds.id));
    if (preview?.id === ds.id) setPreview(null);
  };

  const handleCreatePipeline = async (ds: Dataset) => {
    const name = newPipelineName.trim() || `Pipeline for ${ds.name}`;
    setCreatingPipeline(true);
    try {
      const p = await createPipeline(name, ds.id);
      setActiveDataset(ds);
      setActivePipeline(p);
      navigate(`/pipeline/${p.id}`);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setCreatingPipeline(false);
    }
  };

  const schema = preview ? JSON.parse(preview.schema_json) : {};
  const stats = preview ? JSON.parse(preview.stats_json) : {};
  const sample = preview ? JSON.parse(preview.sample_json) : [];
  const nullCounts = preview
    ? Object.fromEntries(
        Object.entries(stats).map(([col, s]: any) => [col, s.null_count ?? 0])
      )
    : {};

  return (
    <div className="flex h-[calc(100vh-53px)] overflow-hidden">
      {/* ── Left: dataset list + upload ─────────────────────────────────── */}
      <aside className="w-72 flex-shrink-0 border-r border-surface-3 bg-surface-1 flex flex-col">
        <div className="p-4 border-b border-surface-3">
          <h2 className="text-sm font-medium text-gray-200 mb-3">Datasets</h2>

          {/* Drop zone */}
          <div
            className={clsx(
              'border-2 border-dashed rounded-xl p-5 flex flex-col items-center gap-2 cursor-pointer transition-all',
              dragOver
                ? 'border-accent bg-accent/10'
                : 'border-surface-4 hover:border-surface-4 hover:bg-surface-2'
            )}
            onClick={() => inputRef.current?.click()}
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={handleDrop}
          >
            {uploading ? (
              <Loader2 size={20} className="text-accent animate-spin" />
            ) : (
              <Upload size={20} className="text-gray-500" />
            )}
            <p className="text-xs text-gray-500 text-center">
              {uploading ? 'Uploading…' : 'Drop CSV or click to browse'}
            </p>
            <input
              ref={inputRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={(e) => e.target.files?.[0] && handleFile(e.target.files[0])}
            />
          </div>

          {error && (
            <div className="mt-2 flex items-center gap-1.5 text-xs text-red-400">
              <AlertCircle size={12} />
              {error}
            </div>
          )}
        </div>

        {/* Dataset list */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {datasets.length === 0 && (
            <p className="text-xs text-gray-600 text-center mt-8 px-4">
              No datasets yet. Upload a CSV to get started.
            </p>
          )}
          {datasets.map((ds) => (
            <div
              key={ds.id}
              onClick={() => setPreview(ds)}
              className={clsx(
                'group flex items-center gap-2 px-3 py-2.5 rounded-lg cursor-pointer transition-all border',
                preview?.id === ds.id
                  ? 'bg-accent/10 border-accent/30 text-white'
                  : 'bg-transparent border-transparent hover:bg-surface-2 text-gray-300'
              )}
            >
              <FileText size={14} className="flex-shrink-0 text-gray-500" />
              <div className="flex-1 min-w-0">
                <p className="text-xs font-medium truncate">{ds.name}</p>
                <p className="text-xs text-gray-500 font-mono">
                  {ds.row_count.toLocaleString()} × {ds.col_count}
                </p>
              </div>
              <button
                onClick={(e) => handleDelete(ds, e)}
                className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-900/30 text-gray-500 hover:text-red-400 transition-all"
              >
                <Trash2 size={12} />
              </button>
            </div>
          ))}
        </div>
      </aside>

      {/* ── Right: preview ──────────────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {!preview && (
          <div className="flex flex-col items-center justify-center h-full text-gray-600 gap-3">
            <Table size={40} strokeWidth={1} />
            <p className="text-sm">Select a dataset to preview</p>
          </div>
        )}

        {preview && (
          <>
            {/* Header */}
            <div className="flex items-start justify-between">
              <div>
                <h1 className="text-lg font-semibold text-white">{preview.name}</h1>
                <p className="text-sm text-gray-500 font-mono mt-0.5">
                  {preview.row_count.toLocaleString()} rows · {preview.col_count} columns
                </p>
              </div>

              {/* Create pipeline */}
              <div className="flex items-center gap-2">
                <input
                  className="input w-48 text-xs"
                  placeholder="Pipeline name (optional)"
                  value={newPipelineName}
                  onChange={(e) => setNewPipelineName(e.target.value)}
                />
                <button
                  className="btn-primary text-xs"
                  disabled={creatingPipeline}
                  onClick={() => handleCreatePipeline(preview)}
                >
                  {creatingPipeline ? (
                    <Loader2 size={14} className="animate-spin" />
                  ) : (
                    <Plus size={14} />
                  )}
                  New Pipeline
                  <ChevronRight size={14} />
                </button>
              </div>
            </div>

            {/* Schema cards */}
            <div>
              <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
                Schema
              </h2>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2">
                {Object.entries(schema).map(([col, dtype]) => {
                  const s = stats[col] ?? {};
                  const nullPct = preview.row_count
                    ? Math.round(((s.null_count ?? 0) / preview.row_count) * 100)
                    : 0;
                  return (
                    <div key={col} className="card p-3 space-y-1.5">
                      <p className="text-xs font-medium text-gray-200 truncate" title={col}>
                        {col}
                      </p>
                      <span className="badge-muted text-[10px]">{String(dtype)}</span>
                      <div className="flex items-center justify-between text-[10px] text-gray-500 font-mono mt-1">
                        <span>{(s.unique_count ?? 0).toLocaleString()} uniq</span>
                        {nullPct > 0 && (
                          <span className={nullPct > 50 ? 'text-red-400' : 'text-amber-500'}>
                            {nullPct}% null
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Sample rows */}
            <div>
              <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
                Sample Data (first 50 rows)
              </h2>
              <DataTable
                rows={sample}
                schema={schema}
                nullCounts={nullCounts}
                totalRows={preview.row_count}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
