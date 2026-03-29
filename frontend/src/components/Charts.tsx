// Charts.tsx — Recharts visualizations for the debugger page

import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Cell
} from 'recharts';
import type { StepSnapshot } from '../types';

interface Props {
  snapshots: StepSnapshot[];
  activeIndex: number;
}

const COLORS = {
  row: '#3b82f6',
  null: '#f59e0b',
  dup: '#8b5cf6',
  ok: '#22c55e',
  danger: '#ef4444',
};

// ── Custom tooltip ─────────────────────────────────────────────────────────────
function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-surface-2 border border-surface-4 rounded-lg px-3 py-2 text-xs shadow-xl">
      <p className="text-gray-400 mb-1 font-mono">{label}</p>
      {payload.map((p: any) => (
        <p key={p.name} style={{ color: p.color ?? '#fff' }}>
          {p.name}: <span className="font-mono font-medium">{p.value?.toLocaleString()}</span>
        </p>
      ))}
    </div>
  );
}

// ── Row count trend ────────────────────────────────────────────────────────────
export function RowCountChart({ snapshots, activeIndex }: Props) {
  const data = snapshots.map((s) => ({
    name: s.step_name.length > 14 ? s.step_name.slice(0, 13) + '…' : s.step_name,
    rows: s.row_count,
    index: s.step_index,
  }));

  return (
    <div className="card p-4">
      <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
        Row Count Across Steps
      </h3>
      <ResponsiveContainer width="100%" height={160}>
        <AreaChart data={data} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
          <defs>
            <linearGradient id="rowGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={COLORS.row} stopOpacity={0.3} />
              <stop offset="95%" stopColor={COLORS.row} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#252d40" />
          <XAxis dataKey="name" tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <YAxis tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <Tooltip content={<CustomTooltip />} />
          {activeIndex > 0 && (
            <ReferenceLine
              x={data[activeIndex]?.name}
              stroke={COLORS.row}
              strokeDasharray="4 2"
              strokeWidth={1.5}
            />
          )}
          <Area
            type="monotone"
            dataKey="rows"
            stroke={COLORS.row}
            strokeWidth={2}
            fill="url(#rowGrad)"
            dot={{ fill: COLORS.row, r: 3, strokeWidth: 0 }}
            activeDot={{ r: 5, fill: COLORS.row }}
            name="rows"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Null count per column (for current snapshot) ───────────────────────────────
export function NullCountChart({ snapshot }: { snapshot: StepSnapshot }) {
  const nullCounts: Record<string, number> = JSON.parse(snapshot.null_counts_json);
  const data = Object.entries(nullCounts)
    .filter(([, v]) => v > 0)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 10)
    .map(([col, count]) => ({
      name: col.length > 12 ? col.slice(0, 11) + '…' : col,
      nulls: count,
      pct: snapshot.row_count > 0 ? Math.round(count / snapshot.row_count * 100) : 0,
    }));

  if (data.length === 0) {
    return (
      <div className="card p-4">
        <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
          Null Counts
        </h3>
        <p className="text-xs text-gray-600 py-4 text-center">No null values in this snapshot.</p>
      </div>
    );
  }

  return (
    <div className="card p-4">
      <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
        Null Counts (top {data.length} columns)
      </h3>
      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 40, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#252d40" horizontal={false} />
          <XAxis type="number" tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <YAxis dataKey="name" type="category" width={80} tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="nulls" name="nulls" radius={[0, 3, 3, 0]} maxBarSize={14}>
            {data.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.pct > 50 ? COLORS.danger : entry.pct > 20 ? COLORS.null : COLORS.row}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Row delta bar chart across all steps ──────────────────────────────────────
export function RowDeltaChart({ snapshots }: { snapshots: StepSnapshot[] }) {
  const data = snapshots.slice(1).map((s) => {
    const diff = JSON.parse(s.diff_json || '{}');
    const delta = diff.row_delta ?? 0;
    return {
      name: s.step_name.length > 14 ? s.step_name.slice(0, 13) + '…' : s.step_name,
      delta,
      pct: diff.row_delta_pct ?? 0,
    };
  });

  if (data.length === 0) return null;

  return (
    <div className="card p-4">
      <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
        Row Change Per Step
      </h3>
      <ResponsiveContainer width="100%" height={140}>
        <BarChart data={data} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#252d40" />
          <XAxis dataKey="name" tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <YAxis tick={{ fontSize: 9, fill: '#6b7280', fontFamily: 'JetBrains Mono' }} />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine y={0} stroke="#374151" />
          <Bar dataKey="delta" name="Δ rows" radius={[3, 3, 0, 0]} maxBarSize={32}>
            {data.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.delta < 0 ? COLORS.danger : entry.delta > 0 ? COLORS.ok : COLORS.row}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
