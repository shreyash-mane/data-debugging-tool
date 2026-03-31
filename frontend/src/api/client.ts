// api/client.ts — Typed API client for all backend endpoints

import type {
  Dataset,
  Pipeline,
  PipelineStep,
  PipelineRun,
  StepSnapshot,
  StepType,
} from '../types';

const BASE = '/api';

async function req<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text();
    let detail = body;
    try { detail = JSON.parse(body).detail ?? body; } catch { /* ignore */ }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

// ── Datasets ──────────────────────────────────────────────────────────────────

export async function uploadDataset(file: File): Promise<Dataset> {
  const form = new FormData();
  form.append('file', file);
  const res = await fetch(`${BASE}/datasets/upload`, {
    method: 'POST',
    body: form,
  });
  if (!res.ok) {
    const body = await res.json();
    throw new Error(body.detail ?? 'Upload failed');
  }
  return res.json();
}

export const listDatasets = (): Promise<Dataset[]> =>
  req('/datasets');

export const getDataset = (id: number): Promise<Dataset> =>
  req(`/datasets/${id}`);

export const deleteDataset = (id: number): Promise<{ ok: boolean }> =>
  req(`/datasets/${id}`, { method: 'DELETE' });

export const listUploads = (): Promise<{ files: string[] }> =>
  req('/uploads');

// ── Pipelines ─────────────────────────────────────────────────────────────────

export const createPipeline = (name: string, dataset_id: number): Promise<Pipeline> =>
  req('/pipelines', {
    method: 'POST',
    body: JSON.stringify({ name, dataset_id }),
  });

export const listPipelines = (dataset_id?: number): Promise<Pipeline[]> =>
  req(`/pipelines${dataset_id ? `?dataset_id=${dataset_id}` : ''}`);

export const getPipeline = (id: number): Promise<Pipeline> =>
  req(`/pipelines/${id}`);

export const deletePipeline = (id: number): Promise<{ ok: boolean }> =>
  req(`/pipelines/${id}`, { method: 'DELETE' });

// ── Steps ─────────────────────────────────────────────────────────────────────

export const listSteps = (pipeline_id: number): Promise<PipelineStep[]> =>
  req(`/pipelines/${pipeline_id}/steps`);

export const createStep = (
  pipeline_id: number,
  step: {
    name: string;
    step_type: StepType;
    config_json: string;
    order: number;
    enabled?: boolean;
  },
): Promise<PipelineStep> =>
  req(`/pipelines/${pipeline_id}/steps`, {
    method: 'POST',
    body: JSON.stringify({ enabled: true, ...step }),
  });

export const updateStep = (
  step_id: number,
  patch: Partial<{
    name: string;
    step_type: StepType;
    config_json: string;
    order: number;
    enabled: boolean;
  }>,
): Promise<PipelineStep> =>
  req(`/steps/${step_id}`, {
    method: 'PUT',
    body: JSON.stringify(patch),
  });

export const deleteStep = (step_id: number): Promise<{ ok: boolean }> =>
  req(`/steps/${step_id}`, { method: 'DELETE' });

export const reorderSteps = (
  pipeline_id: number,
  steps: { step_id: number; order: number }[],
): Promise<{ ok: boolean }> =>
  req(`/pipelines/${pipeline_id}/reorder`, {
    method: 'POST',
    body: JSON.stringify({ steps }),
  });

// ── Runs ──────────────────────────────────────────────────────────────────────

export const runPipeline = (pipeline_id: number): Promise<PipelineRun> =>
  req(`/pipelines/${pipeline_id}/run`, { method: 'POST' });

export const getRun = (run_id: number): Promise<PipelineRun> =>
  req(`/runs/${run_id}`);

export const listRuns = (pipeline_id: number): Promise<PipelineRun[]> =>
  req(`/pipelines/${pipeline_id}/runs`);

export const listSnapshots = (run_id: number): Promise<StepSnapshot[]> =>
  req(`/runs/${run_id}/snapshots`);

export const getSnapshot = (snapshot_id: number): Promise<StepSnapshot> =>
  req(`/snapshots/${snapshot_id}`);
