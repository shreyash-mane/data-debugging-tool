// store/useAppStore.ts — Zustand global state

import { create } from 'zustand';
import type { Dataset, Pipeline, PipelineStep, PipelineRun, StepSnapshot } from '../types';

interface AppState {
  // Selected objects
  activeDataset: Dataset | null;
  activePipeline: Pipeline | null;
  activeRun: PipelineRun | null;
  activeStepIndex: number; // which snapshot is selected in the debugger

  // Lists
  datasets: Dataset[];
  pipelines: Pipeline[];
  steps: PipelineStep[];
  snapshots: StepSnapshot[];

  // UI state
  isRunning: boolean;
  runError: string | null;

  // Setters
  setActiveDataset: (d: Dataset | null) => void;
  setActivePipeline: (p: Pipeline | null) => void;
  setActiveRun: (r: PipelineRun | null) => void;
  setActiveStepIndex: (i: number) => void;

  setDatasets: (ds: Dataset[]) => void;
  setPipelines: (ps: Pipeline[]) => void;
  setSteps: (ss: PipelineStep[]) => void;
  setSnapshots: (ss: StepSnapshot[]) => void;

  setIsRunning: (v: boolean) => void;
  setRunError: (e: string | null) => void;

  // Convenience: add / update / remove a step in the local list
  upsertStep: (step: PipelineStep) => void;
  removeStep: (step_id: number) => void;

  reset: () => void;
}

const initialState = {
  activeDataset: null,
  activePipeline: null,
  activeRun: null,
  activeStepIndex: 0,
  datasets: [],
  pipelines: [],
  steps: [],
  snapshots: [],
  isRunning: false,
  runError: null,
};

export const useAppStore = create<AppState>((set, get) => ({
  ...initialState,

  setActiveDataset: (d) => set({ activeDataset: d }),
  setActivePipeline: (p) => set({ activePipeline: p }),
  setActiveRun: (r) => set({ activeRun: r }),
  setActiveStepIndex: (i) => set({ activeStepIndex: i }),

  setDatasets: (ds) => set({ datasets: ds }),
  setPipelines: (ps) => set({ pipelines: ps }),
  setSteps: (ss) => set({ steps: [...ss].sort((a, b) => a.order - b.order) }),
  setSnapshots: (ss) => set({ snapshots: [...ss].sort((a, b) => a.step_index - b.step_index) }),

  setIsRunning: (v) => set({ isRunning: v }),
  setRunError: (e) => set({ runError: e }),

  upsertStep: (step) => {
    const existing = get().steps;
    const idx = existing.findIndex((s) => s.id === step.id);
    let updated: PipelineStep[];
    if (idx >= 0) {
      updated = [...existing];
      updated[idx] = step;
    } else {
      updated = [...existing, step];
    }
    set({ steps: updated.sort((a, b) => a.order - b.order) });
  },

  removeStep: (step_id) => {
    set({ steps: get().steps.filter((s) => s.id !== step_id) });
  },

  reset: () => set(initialState),
}));
