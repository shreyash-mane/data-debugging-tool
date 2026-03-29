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
  let res: Response;
  try {
    res = await fetch(`${BASE}${path}`, {
      headers: { 'Content-Type': 'application/json', ...options.headers },
      ...options,
    });
  } catch (networkErr) {
    throw new Error(
      'Cannot reach the backend. Make sure the FastAPI server is running on port 8000.\n' +
      'Run: cd backend && uvicorn main:app --reload --port 8000'
    );
  }

  // Try to parse as JSON regardless of status, fall back to text
  const contentType = res.headers.get('content-type') ?? '';
  const isJson = contentType.includes('application/json');

  if (!res.ok) {
    if (isJson) {
      const body = await res.json();
      throw new Error(body.detail ?? JSON.stringify(body));
    } else {
      const text = await res.text();
      // Truncate raw HTML error pages to something readable
      const clean = text.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim().slice(0, 200);
      throw new Error(`Server error ${res.status}: ${clean || res.statusText}`);
    }
  }

  if (!isJson) {
    // Endpoint returned non-JSON on success (shouldn't happen, but guard anyway)
    const text = await res.text();
    try { return JSON.parse(text) as T; } catch {
      throw new Error(`Expected JSON from ${path} but got: ${text.slice(0, 100)}`);
    }
  }

  return res.json() as Promise<T>;
}

// ── Datasets ──────────────────────────────────────────────────────────────────

export async function uploadDataset(file: File): Promise<Dataset> {
  const form = new FormData();
  form.append('file', file);
  let res: Response;
  try {
    res = await fetch(`${BASE}/datasets/upload`, { method: 'POST', body: form });
  } catch {
    throw new Error('Cannot reach the backend. Make sure the FastAPI server is running on port 8000.');
  }
  if (!res.ok) {
    const contentType = res.headers.get('content-type') ?? '';
    if (contentType.includes('application/json')) {
      const body = await res.json();
      throw new Error(body.detail ?? JSON.stringify(body));
    }
    const text = await res.text();
    throw new Error(`Upload failed (${res.status}): ${text.replace(/<[^>]+>/g, '').trim().slice(0, 200)}`);
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
