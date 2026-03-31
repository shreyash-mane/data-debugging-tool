// StepEditor.tsx — Form to create/edit a pipeline step
// Each step type shows relevant config fields.

import { useState, useEffect } from 'react';
import type { PipelineStep, StepType } from '../types';
import clsx from 'clsx';

const STEP_TYPES: { value: StepType; label: string; description: string; recommended?: boolean }[] = [
  { value: 'auto_clean', label: 'Auto Clean', description: 'Intelligent: detect types, strip symbols, fill missing (skewness-based)', recommended: true },
  { value: 'drop_missing', label: 'Drop Missing', description: 'Remove rows with null values' },
  { value: 'fill_missing', label: 'Fill Missing', description: 'Replace nulls with a value or statistic' },
  { value: 'rename_column', label: 'Rename Column', description: 'Rename one or more columns' },
  { value: 'change_dtype', label: 'Change Data Type', description: 'Cast a column to a new type' },
  { value: 'filter_rows', label: 'Filter Rows', description: 'Keep rows matching a condition' },
  { value: 'select_columns', label: 'Select Columns', description: 'Keep only specified columns' },
  { value: 'sort_values', label: 'Sort Values', description: 'Sort rows by one or more columns' },
  { value: 'remove_duplicates', label: 'Remove Duplicates', description: 'Drop duplicate rows' },
  { value: 'add_computed_column', label: 'Add Computed Column', description: 'Create a new column from existing ones' },
  { value: 'join', label: 'Join Dataset', description: 'Merge with a second uploaded CSV' },
  { value: 'group_aggregate', label: 'Group & Aggregate', description: 'GroupBy + aggregate functions' },
];

interface Props {
  columns: string[];           // columns available at this point in the pipeline
  uploadedFiles: string[];     // for join step
  step?: PipelineStep | null;  // existing step to edit (null = create mode)
  nextOrder: number;
  onSave: (data: { name: string; step_type: StepType; config_json: string; order: number }) => void;
  onCancel: () => void;
  saving?: boolean;
}

export default function StepEditor({
  columns, uploadedFiles, step, nextOrder, onSave, onCancel, saving
}: Props) {
  const [name, setName] = useState(step?.name ?? '');
  const [stepType, setStepType] = useState<StepType>(step?.step_type ?? 'filter_rows');
  const [config, setConfig] = useState<Record<string, any>>({});

  // Parse existing config when editing
  useEffect(() => {
    if (step) {
      setName(step.name);
      setStepType(step.step_type);
      try { setConfig(JSON.parse(step.config_json)); } catch { setConfig({}); }
    } else {
      setConfig({});
    }
  }, [step]);

  // Reset config when step type changes
  const handleTypeChange = (t: StepType) => {
    setStepType(t);
    setConfig({});
    const meta = STEP_TYPES.find(s => s.value === t);
    if (meta && !name) setName(meta.label);
  };

  const setField = (k: string, v: any) => setConfig((c) => ({ ...c, [k]: v }));

  const handleSave = () => {
    onSave({
      name: name || (STEP_TYPES.find(s => s.value === stepType)?.label ?? stepType),
      step_type: stepType,
      config_json: JSON.stringify(config),
      order: step?.order ?? nextOrder,
    });
  };

  return (
    <div className="space-y-4">
      {/* Step name */}
      <div>
        <label className="label">Step Name</label>
        <input
          className="input"
          placeholder="e.g. Remove nulls in salary"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </div>

      {/* Step type selector */}
      <div>
        <label className="label">Transformation Type</label>
        <div className="grid grid-cols-2 gap-1.5 max-h-52 overflow-y-auto pr-1">
          {STEP_TYPES.map((t) => (
            <button
              key={t.value}
              type="button"
              onClick={() => handleTypeChange(t.value)}
              className={clsx(
                'text-left px-3 py-2 rounded-lg border text-xs transition-all',
                stepType === t.value
                  ? 'bg-accent/15 border-accent/50 text-white'
                  : t.recommended
                  ? 'bg-ok/5 border-ok/30 text-gray-300 hover:border-ok/50 hover:text-white'
                  : 'bg-surface-2 border-surface-4 text-gray-400 hover:border-surface-4 hover:text-gray-200'
              )}
            >
              <div className="font-medium flex items-center gap-1">
                {t.label}
                {t.recommended && (
                  <span className="text-[9px] px-1 py-0.5 rounded bg-ok/20 text-ok font-semibold tracking-wide">
                    AUTO
                  </span>
                )}
              </div>
              <div className="text-[10px] text-gray-500 mt-0.5">{t.description}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Step-specific config fields */}
      <ConfigFields
        stepType={stepType}
        config={config}
        setField={setField}
        columns={columns}
        uploadedFiles={uploadedFiles}
      />

      {/* Actions */}
      <div className="flex gap-2 pt-2">
        <button className="btn-primary flex-1" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving…' : step ? 'Update Step' : 'Add Step'}
        </button>
        <button className="btn-ghost" onClick={onCancel}>Cancel</button>
      </div>
    </div>
  );
}

// ── Per-step config sub-forms ──────────────────────────────────────────────────

function ColSelect({ label, value, onChange, columns, multi = false }: {
  label: string; value: any; onChange: (v: any) => void;
  columns: string[]; multi?: boolean;
}) {
  if (multi) {
    const selected: string[] = Array.isArray(value) ? value : [];
    const toggle = (col: string) => {
      if (selected.includes(col)) onChange(selected.filter(c => c !== col));
      else onChange([...selected, col]);
    };
    return (
      <div>
        <label className="label">{label}</label>
        <div className="flex flex-wrap gap-1 max-h-28 overflow-y-auto p-2 bg-surface-2 rounded-lg border border-surface-4">
          {columns.map(col => (
            <button
              key={col}
              type="button"
              onClick={() => toggle(col)}
              className={clsx(
                'px-2 py-0.5 rounded text-[11px] font-mono transition-all border',
                selected.includes(col)
                  ? 'bg-accent/20 border-accent/50 text-accent'
                  : 'bg-surface-3 border-surface-4 text-gray-400 hover:text-gray-200'
              )}
            >
              {col}
            </button>
          ))}
        </div>
        {selected.length > 0 && (
          <p className="text-[10px] text-gray-500 mt-1 font-mono">
            Selected: {selected.join(', ')}
          </p>
        )}
      </div>
    );
  }
  return (
    <div>
      <label className="label">{label}</label>
      <select className="select" value={value ?? ''} onChange={(e) => onChange(e.target.value)}>
        <option value="">— choose column —</option>
        {columns.map(c => <option key={c} value={c}>{c}</option>)}
      </select>
    </div>
  );
}

function ConfigFields({ stepType, config, setField, columns, uploadedFiles }: {
  stepType: StepType; config: Record<string, any>;
  setField: (k: string, v: any) => void;
  columns: string[]; uploadedFiles: string[];
}) {
  switch (stepType) {
    case 'auto_clean':
      return (
        <div className="space-y-3">
          <div className="p-3 bg-ok/5 border border-ok/25 rounded-lg space-y-1.5">
            <p className="text-xs font-semibold text-ok">Intelligent Auto-Clean — zero config required</p>
            <ul className="text-[11px] text-gray-400 space-y-0.5 list-none">
              <li>• Detects column types automatically (numeric / categorical / datetime / text)</li>
              <li>• Strips currency symbols (£ $ €) and commas from numeric columns</li>
              <li>• Normalises mixed datetime formats; invalid dates → NaT</li>
              <li>• Fills missing values: mean (symmetric) or median (skewed) for numeric; mode or "Unknown" for categorical</li>
              <li>• Drops columns with &gt;40% missing (configurable below)</li>
            </ul>
          </div>
          <ColSelect
            label="Columns to clean (leave empty = ALL columns)"
            value={config.columns}
            onChange={(v) => setField('columns', v)}
            columns={columns}
            multi
          />
          <div>
            <label className="label">Drop columns with &gt;40% missing?</label>
            <select
              className="select"
              value={String(config.drop_columns_above_threshold ?? 'true')}
              onChange={(e) => setField('drop_columns_above_threshold', e.target.value === 'true')}
            >
              <option value="true">Yes — drop them (recommended)</option>
              <option value="false">No — keep them (fill with Unknown/NaN)</option>
            </select>
          </div>
        </div>
      );

    case 'drop_missing':
      return (
        <div className="space-y-3">
          <ColSelect label="Columns (leave empty = all)" value={config.columns} onChange={(v) => setField('columns', v)} columns={columns} multi />
          <div>
            <label className="label">How</label>
            <select className="select" value={config.how ?? 'any'} onChange={(e) => setField('how', e.target.value)}>
              <option value="any">any — drop if any selected column is null</option>
              <option value="all">all — drop only if ALL selected columns are null</option>
            </select>
          </div>
        </div>
      );

    case 'fill_missing':
      return (
        <div className="space-y-3">
          <ColSelect label="Column *" value={config.column} onChange={(v) => setField('column', v)} columns={columns} />
          <div>
            <label className="label">Fill Method</label>
            <select className="select" value={config.method ?? 'auto'} onChange={(e) => setField('method', e.target.value)}>
              <option value="auto">Auto (skewness-based mean or median)</option>
              <option value="value">Constant value</option>
              <option value="mean">Mean</option>
              <option value="median">Median</option>
              <option value="mode">Mode (most frequent)</option>
              <option value="ffill">Forward fill</option>
              <option value="bfill">Backward fill</option>
            </select>
          </div>
          {config.method === 'value' && (
            <div>
              <label className="label">Fill Value</label>
              <input className="input" placeholder="e.g. 0 or Unknown" value={config.value ?? ''} onChange={(e) => setField('value', e.target.value)} />
            </div>
          )}
          {(!config.method || config.method === 'auto') && (
            <p className="text-[10px] text-ok/80 bg-ok/5 border border-ok/20 rounded px-2 py-1.5">
              AUTO: numeric columns use mean (|skew| &lt; 0.5) or median; categorical uses mode or "Unknown"
            </p>
          )}
        </div>
      );

    case 'rename_column': {
      const mappings: Record<string, string> = config.mappings ?? {};
      return (
        <div className="space-y-2">
          <label className="label">Column Renames (old → new)</label>
          {columns.map(col => (
            <div key={col} className="flex items-center gap-2">
              <span className="text-xs font-mono text-gray-400 w-36 truncate flex-shrink-0">{col}</span>
              <span className="text-gray-600 text-xs">→</span>
              <input
                className="input text-xs"
                placeholder="new name (leave blank to keep)"
                value={mappings[col] ?? ''}
                onChange={(e) => {
                  const newMap = { ...mappings };
                  if (e.target.value) newMap[col] = e.target.value;
                  else delete newMap[col];
                  setField('mappings', newMap);
                }}
              />
            </div>
          ))}
        </div>
      );
    }

    case 'change_dtype':
      return (
        <div className="space-y-3">
          <ColSelect label="Column *" value={config.column} onChange={(v) => setField('column', v)} columns={columns} />
          <div>
            <label className="label">Target Type</label>
            <select className="select" value={config.dtype ?? 'float'} onChange={(e) => setField('dtype', e.target.value)}>
              <option value="int">Integer (Int64)</option>
              <option value="float">Float</option>
              <option value="str">String</option>
              <option value="bool">Boolean</option>
              <option value="datetime">Datetime</option>
            </select>
          </div>
        </div>
      );

    case 'filter_rows':
      return (
        <div className="space-y-3">
          <ColSelect label="Column *" value={config.column} onChange={(v) => setField('column', v)} columns={columns} />
          <div>
            <label className="label">Operator</label>
            <select className="select" value={config.operator ?? '=='} onChange={(e) => setField('operator', e.target.value)}>
              <option value="==">== equals</option>
              <option value="!=">!= not equals</option>
              <option value=">">{'>'} greater than</option>
              <option value=">=">{'>='} greater or equal</option>
              <option value="<">{'<'} less than</option>
              <option value="<=">{'<='} less or equal</option>
              <option value="contains">contains (string)</option>
              <option value="startswith">starts with (string)</option>
              <option value="isnull">is null</option>
              <option value="notnull">is not null</option>
            </select>
          </div>
          {!['isnull', 'notnull'].includes(config.operator) && (
            <div>
              <label className="label">Value</label>
              <input className="input" placeholder="e.g. 1000 or North" value={config.value ?? ''} onChange={(e) => setField('value', e.target.value)} />
            </div>
          )}
        </div>
      );

    case 'select_columns':
      return (
        <ColSelect label="Columns to keep *" value={config.columns} onChange={(v) => setField('columns', v)} columns={columns} multi />
      );

    case 'sort_values':
      return (
        <div className="space-y-3">
          <ColSelect label="Sort by columns *" value={config.columns} onChange={(v) => setField('columns', v)} columns={columns} multi />
          <div>
            <label className="label">Direction</label>
            <select className="select" value={String(config.ascending ?? 'true')} onChange={(e) => setField('ascending', e.target.value === 'true')}>
              <option value="true">Ascending</option>
              <option value="false">Descending</option>
            </select>
          </div>
        </div>
      );

    case 'remove_duplicates':
      return (
        <div className="space-y-3">
          <ColSelect label="Subset columns (empty = all)" value={config.columns} onChange={(v) => setField('columns', v)} columns={columns} multi />
          <div>
            <label className="label">Keep</label>
            <select className="select" value={config.keep ?? 'first'} onChange={(e) => setField('keep', e.target.value)}>
              <option value="first">Keep first occurrence</option>
              <option value="last">Keep last occurrence</option>
              <option value="false">Drop all duplicates</option>
            </select>
          </div>
        </div>
      );

    case 'add_computed_column':
      return (
        <div className="space-y-3">
          <div>
            <label className="label">New Column Name *</label>
            <input className="input" placeholder="e.g. profit_margin" value={config.new_column ?? ''} onChange={(e) => setField('new_column', e.target.value)} />
          </div>
          <div>
            <label className="label">Operation</label>
            <select className="select" value={config.operation ?? 'add'} onChange={(e) => setField('operation', e.target.value)}>
              <option value="add">Add (col_a + col_b or constant)</option>
              <option value="subtract">Subtract (col_a - col_b or constant)</option>
              <option value="multiply">Multiply (col_a × col_b or constant)</option>
              <option value="divide">Divide (col_a ÷ col_b or constant)</option>
              <option value="concat">Concatenate strings</option>
              <option value="constant">Set constant value</option>
            </select>
          </div>
          {config.operation !== 'constant' && (
            <ColSelect label="Column A *" value={config.col_a} onChange={(v) => setField('col_a', v)} columns={columns} />
          )}
          {config.operation !== 'constant' && (
            <div className="space-y-2">
              <ColSelect label="Column B (or leave empty to use constant)" value={config.col_b} onChange={(v) => setField('col_b', v)} columns={['', ...columns]} />
              {!config.col_b && (
                <div>
                  <label className="label">Constant Value</label>
                  <input className="input" placeholder="e.g. 100" value={config.constant_value ?? ''} onChange={(e) => setField('constant_value', e.target.value)} />
                </div>
              )}
            </div>
          )}
          {config.operation === 'constant' && (
            <div>
              <label className="label">Constant Value</label>
              <input className="input" placeholder="e.g. 0" value={config.constant_value ?? ''} onChange={(e) => setField('constant_value', e.target.value)} />
            </div>
          )}
        </div>
      );

    case 'join':
      return (
        <div className="space-y-3">
          <div>
            <label className="label">Right Dataset (uploaded CSV filename) *</label>
            <select className="select" value={config.right_dataset_path ?? ''} onChange={(e) => setField('right_dataset_path', e.target.value)}>
              <option value="">— select uploaded file —</option>
              {uploadedFiles.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
            {uploadedFiles.length === 0 && (
              <p className="text-[10px] text-amber-500 mt-1">Upload a second CSV first to use as join dataset.</p>
            )}
          </div>
          <div>
            <label className="label">Join Key(s) *</label>
            <input className="input" placeholder="e.g. customer_id (comma-separate for multiple)" value={Array.isArray(config.on) ? config.on.join(', ') : (config.on ?? '')} onChange={(e) => {
              const val = e.target.value.split(',').map(s => s.trim()).filter(Boolean);
              setField('on', val.length === 1 ? val[0] : val);
            }} />
          </div>
          <div>
            <label className="label">Join Type</label>
            <select className="select" value={config.how ?? 'inner'} onChange={(e) => setField('how', e.target.value)}>
              <option value="inner">Inner (keep matching rows only)</option>
              <option value="left">Left (keep all left rows)</option>
              <option value="right">Right (keep all right rows)</option>
              <option value="outer">Outer (keep all rows)</option>
            </select>
          </div>
        </div>
      );

    case 'group_aggregate': {
      const aggs: Record<string, string> = config.aggregations ?? {};
      return (
        <div className="space-y-3">
          <ColSelect label="Group By Columns *" value={config.group_by} onChange={(v) => setField('group_by', v)} columns={columns} multi />
          <div>
            <label className="label">Aggregations per Column</label>
            <div className="space-y-1.5 max-h-48 overflow-y-auto pr-1">
              {columns
                .filter(c => !(config.group_by ?? []).includes(c))
                .map(col => (
                  <div key={col} className="flex items-center gap-2">
                    <span className="text-xs font-mono text-gray-400 w-36 truncate flex-shrink-0">{col}</span>
                    <select
                      className="select text-xs py-1"
                      value={aggs[col] ?? ''}
                      onChange={(e) => {
                        const newAggs = { ...aggs };
                        if (e.target.value) newAggs[col] = e.target.value;
                        else delete newAggs[col];
                        setField('aggregations', newAggs);
                      }}
                    >
                      <option value="">— skip —</option>
                      <option value="sum">sum</option>
                      <option value="mean">mean</option>
                      <option value="count">count</option>
                      <option value="min">min</option>
                      <option value="max">max</option>
                      <option value="first">first</option>
                      <option value="last">last</option>
                    </select>
                  </div>
                ))}
            </div>
          </div>
        </div>
      );
    }

    default:
      return <p className="text-xs text-gray-500">No configuration needed for this step type.</p>;
  }
}
