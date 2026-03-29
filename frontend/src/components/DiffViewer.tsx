// DiffViewer.tsx — Side-by-side diff between consecutive steps

import type { DiffData } from '../types';
import clsx from 'clsx';
import { ArrowDown, ArrowUp, Minus } from 'lucide-react';

interface Props {
  diff: DiffData;
  stepName: string;
}

function DeltaBadge({ value, isPercent = false, invertColor = false }: {
  value: number | null; isPercent?: boolean; invertColor?: boolean;
}) {
  if (value === null || value === undefined) return <span className="text-gray-600">—</span>;
  const positive = value > 0;
  const isGood = invertColor ? !positive : positive;
  return (
    <span className={clsx(
      'inline-flex items-center gap-0.5 font-mono text-[11px] font-medium',
      positive ? (isGood ? 'text-ok' : 'text-red-400') : (isGood ? 'text-ok' : 'text-red-400'),
      value === 0 && 'text-gray-500'
    )}>
      {value > 0 ? <ArrowUp size={9} /> : value < 0 ? <ArrowDown size={9} /> : <Minus size={9} />}
      {isPercent
        ? `${Math.abs(value).toFixed(1)}%`
        : Math.abs(value).toLocaleString()}
    </span>
  );
}

export default function DiffViewer({ diff, stepName }: Props) {
  if (!diff || Object.keys(diff).length === 0) {
    return <p className="text-xs text-gray-600 py-4 text-center">No diff data (source step).</p>;
  }

  return (
    <div className="space-y-4 text-xs">
      {/* ── Row & column summary ────────────────────────────────────────── */}
      <div className="grid grid-cols-2 gap-3">
        <div className="card p-3 space-y-2">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider">Rows</p>
          <div className="flex items-baseline gap-2">
            <span className="text-white font-mono text-lg font-medium">
              {(diff.row_count_after ?? 0).toLocaleString()}
            </span>
            <DeltaBadge value={diff.row_delta} />
            <DeltaBadge value={diff.row_delta_pct} isPercent invertColor />
          </div>
          <p className="text-gray-600 font-mono text-[10px]">
            before: {(diff.row_count_before ?? 0).toLocaleString()}
          </p>
        </div>

        <div className="card p-3 space-y-2">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider">Columns</p>
          <div className="flex items-baseline gap-2">
            <span className="text-white font-mono text-lg font-medium">
              {diff.col_count_after ?? 0}
            </span>
            <DeltaBadge value={(diff.col_count_after ?? 0) - (diff.col_count_before ?? 0)} />
          </div>
          <p className="text-gray-600 font-mono text-[10px]">
            before: {diff.col_count_before ?? 0}
          </p>
        </div>
      </div>

      {/* ── Columns added/removed ───────────────────────────────────────── */}
      {((diff.columns_added?.length ?? 0) > 0 || (diff.columns_removed?.length ?? 0) > 0) && (
        <div className="card p-3 space-y-2">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider">Column Changes</p>
          {diff.columns_added?.map(col => (
            <div key={col} className="flex items-center gap-1.5 text-ok">
              <ArrowUp size={10} /> <span className="font-mono">{col}</span>
              <span className="text-gray-600">(added)</span>
            </div>
          ))}
          {diff.columns_removed?.map(col => (
            <div key={col} className="flex items-center gap-1.5 text-red-400">
              <ArrowDown size={10} /> <span className="font-mono">{col}</span>
              <span className="text-gray-600">(removed)</span>
            </div>
          ))}
        </div>
      )}

      {/* ── Type changes ───────────────────────────────────────────────── */}
      {Object.keys(diff.type_changes ?? {}).length > 0 && (
        <div className="card p-3 space-y-2">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider">Type Changes</p>
          {Object.entries(diff.type_changes ?? {}).map(([col, tc]) => (
            <div key={col} className="flex items-center gap-2">
              <span className="font-mono text-gray-300">{col}</span>
              <span className="badge-muted">{tc.before}</span>
              <span className="text-gray-600">→</span>
              <span className="badge-info">{tc.after}</span>
            </div>
          ))}
        </div>
      )}

      {/* ── Null changes ────────────────────────────────────────────────── */}
      {Object.keys(diff.null_changes ?? {}).length > 0 && (
        <div className="card p-3">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider mb-2">Null Changes</p>
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-gray-500 border-b border-surface-3">
                  <th className="text-left pb-1.5 font-medium">Column</th>
                  <th className="text-right pb-1.5 font-medium">Before</th>
                  <th className="text-right pb-1.5 font-medium">After</th>
                  <th className="text-right pb-1.5 font-medium">Δ</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(diff.null_changes ?? {})
                  .filter(([, nc]) => (nc.delta ?? 0) !== 0)
                  .sort(([, a], [, b]) => Math.abs(b.delta ?? 0) - Math.abs(a.delta ?? 0))
                  .slice(0, 10)
                  .map(([col, nc]) => (
                    <tr key={col} className="border-b border-surface-3/30">
                      <td className="py-1 font-mono text-gray-300">{col}</td>
                      <td className="py-1 text-right font-mono text-gray-500">{nc.before ?? '—'}</td>
                      <td className="py-1 text-right font-mono">{nc.after}</td>
                      <td className="py-1 text-right">
                        <DeltaBadge value={nc.delta ?? null} invertColor />
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Duplicate changes ───────────────────────────────────────────── */}
      {(diff.duplicate_delta ?? 0) !== 0 && (
        <div className="card p-3 flex items-center gap-3">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider flex-1">Duplicates</p>
          <span className="font-mono text-gray-400">{diff.duplicate_before} →</span>
          <span className="font-mono text-gray-200">{diff.duplicate_after}</span>
          <DeltaBadge value={diff.duplicate_delta} invertColor />
        </div>
      )}

      {/* ── Stat drift (numeric columns) ─────────────────────────────────── */}
      {Object.keys(diff.stat_drift ?? {}).length > 0 && (
        <div className="card p-3">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider mb-2">Mean Drift (numeric)</p>
          <div className="space-y-1.5">
            {Object.entries(diff.stat_drift ?? {})
              .filter(([, sd]) => sd.drift_pct !== null && Math.abs(sd.drift_pct!) > 1)
              .sort(([, a], [, b]) => Math.abs(b.drift_pct ?? 0) - Math.abs(a.drift_pct ?? 0))
              .slice(0, 8)
              .map(([col, sd]) => (
                <div key={col} className="flex items-center gap-2">
                  <span className="font-mono text-gray-300 w-28 truncate">{col}</span>
                  <span className="text-gray-500 font-mono text-[10px]">
                    {sd.mean_before?.toFixed(3) ?? '—'}
                  </span>
                  <span className="text-gray-600">→</span>
                  <span className="font-mono text-[10px]">
                    {sd.mean_after?.toFixed(3) ?? '—'}
                  </span>
                  <DeltaBadge value={sd.drift_pct ?? null} isPercent invertColor />
                </div>
              ))}
          </div>
        </div>
      )}

      {/* ── Category shifts ─────────────────────────────────────────────── */}
      {Object.keys(diff.category_shifts ?? {}).length > 0 && (
        <div className="card p-3 space-y-2">
          <p className="text-gray-400 uppercase text-[10px] tracking-wider">Category Shifts</p>
          {Object.entries(diff.category_shifts ?? {}).map(([col, cs]) => (
            <div key={col} className="space-y-1">
              <p className="font-mono text-gray-300">{col}</p>
              {cs.added_values.length > 0 && (
                <p className="text-ok">
                  + {cs.added_values.slice(0, 5).join(', ')}
                  {cs.added_values.length > 5 ? `… (+${cs.added_values.length - 5} more)` : ''}
                </p>
              )}
              {cs.removed_values.length > 0 && (
                <p className="text-red-400">
                  − {cs.removed_values.slice(0, 5).join(', ')}
                  {cs.removed_values.length > 5 ? `… (${cs.removed_values.length - 5} more)` : ''}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
