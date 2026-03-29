// DataTable.tsx — Scrollable data table with null highlighting and type badges

import clsx from 'clsx';

interface Props {
  rows: Record<string, unknown>[];
  schema?: Record<string, string>;
  nullCounts?: Record<string, number>;
  totalRows?: number;
  maxHeight?: string;
  highlightColumns?: string[];   // columns to highlight (e.g. changed columns)
  caption?: string;
}

const DTYPE_COLOR: Record<string, string> = {
  int64: 'text-blue-400',
  float64: 'text-cyan-400',
  object: 'text-green-400',
  bool: 'text-purple-400',
  datetime64: 'text-orange-400',
  'datetime64[ns]': 'text-orange-400',
};

export default function DataTable({
  rows,
  schema = {},
  nullCounts = {},
  totalRows,
  maxHeight = '340px',
  highlightColumns = [],
  caption,
}: Props) {
  if (!rows || rows.length === 0) {
    return (
      <div className="card p-6 text-center text-sm text-gray-600">
        No rows to display.
      </div>
    );
  }

  const columns = Object.keys(rows[0]);

  return (
    <div className="card overflow-hidden">
      {caption && (
        <div className="px-4 py-2 border-b border-surface-3 text-xs text-gray-400 font-mono">
          {caption}
        </div>
      )}
      <div className="overflow-auto" style={{ maxHeight }}>
        <table className="w-full text-left border-collapse">
          <thead className="sticky top-0 z-10">
            <tr className="bg-surface-2 border-b border-surface-3">
              {columns.map((col) => {
                const dtype = schema[col];
                const nullCount = nullCounts[col] ?? 0;
                const isHighlighted = highlightColumns.includes(col);
                return (
                  <th
                    key={col}
                    className={clsx(
                      'px-3 py-2 text-xs font-medium whitespace-nowrap border-r border-surface-3 last:border-r-0',
                      isHighlighted ? 'bg-accent/10 text-accent' : 'text-gray-300'
                    )}
                  >
                    <div className="flex items-center gap-1.5">
                      <span>{col}</span>
                      {dtype && (
                        <span
                          className={clsx(
                            'text-[9px] font-mono opacity-70',
                            DTYPE_COLOR[dtype] ?? 'text-gray-500'
                          )}
                        >
                          {dtype}
                        </span>
                      )}
                    </div>
                    {nullCount > 0 && (
                      <div className="text-[9px] font-mono text-amber-600 mt-0.5">
                        {nullCount} null
                      </div>
                    )}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr
                key={ri}
                className="border-b border-surface-3/50 hover:bg-surface-2/40 transition-colors"
              >
                {columns.map((col) => {
                  const val = row[col];
                  const isNull = val === null || val === undefined || val === '';
                  const isHighlighted = highlightColumns.includes(col);
                  return (
                    <td
                      key={col}
                      className={clsx(
                        'data-cell border-r border-surface-3/30 last:border-r-0',
                        isNull && 'data-null',
                        isHighlighted && 'bg-accent/5'
                      )}
                    >
                      {isNull ? (
                        <span className="text-gray-600 italic text-[11px]">null</span>
                      ) : typeof val === 'boolean' ? (
                        <span className={val ? 'text-green-400' : 'text-red-400'}>
                          {String(val)}
                        </span>
                      ) : (
                        String(val)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalRows !== undefined && (
        <div className="px-4 py-2 border-t border-surface-3 text-xs text-gray-500 font-mono">
          Showing {rows.length} of {totalRows.toLocaleString()} rows
        </div>
      )}
    </div>
  );
}
