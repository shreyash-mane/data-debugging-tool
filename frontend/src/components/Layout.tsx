import { Outlet, NavLink, useNavigate } from 'react-router-dom';
import { Bug, Database, GitBranch, ChevronRight } from 'lucide-react';
import { useAppStore } from '../store/useAppStore';
import clsx from 'clsx';

export default function Layout() {
  const { activeDataset, activePipeline, activeRun } = useAppStore();
  const navigate = useNavigate();

  return (
    <div className="min-h-screen flex flex-col">
      {/* ── Top nav ─────────────────────────────────────────────────────── */}
      <header className="bg-surface-1 border-b border-surface-3 px-6 py-3 flex items-center gap-6 sticky top-0 z-50">
        {/* Brand */}
        <div
          className="flex items-center gap-2 cursor-pointer select-none"
          onClick={() => navigate('/upload')}
        >
          <div className="w-7 h-7 bg-accent rounded-lg flex items-center justify-center">
            <Bug size={15} className="text-white" />
          </div>
          <span className="font-mono font-medium text-white text-sm tracking-tight">
            DataDebugger
          </span>
        </div>

        {/* Breadcrumb */}
        <nav className="flex items-center gap-1 text-xs text-gray-500 font-mono">
          <NavLink
            to="/upload"
            className={({ isActive }) =>
              clsx('hover:text-gray-300 transition-colors', isActive && 'text-gray-300')
            }
          >
            datasets
          </NavLink>

          {activeDataset && (
            <>
              <ChevronRight size={12} />
              <span className="text-gray-400 truncate max-w-[140px]">
                {activeDataset.name}
              </span>
            </>
          )}

          {activePipeline && (
            <>
              <ChevronRight size={12} />
              <NavLink
                to={`/pipeline/${activePipeline.id}`}
                className={({ isActive }) =>
                  clsx('hover:text-gray-300 transition-colors', isActive && 'text-gray-300')
                }
              >
                {activePipeline.name}
              </NavLink>
            </>
          )}

          {activeRun && (
            <>
              <ChevronRight size={12} />
              <span className="text-accent">debug run #{activeRun.id}</span>
            </>
          )}
        </nav>

        {/* Status indicators */}
        <div className="ml-auto flex items-center gap-3">
          {activeDataset && (
            <div className="flex items-center gap-1.5 text-xs text-gray-500">
              <Database size={12} />
              <span className="font-mono">
                {activeDataset.row_count.toLocaleString()} rows
              </span>
            </div>
          )}
          {activePipeline && (
            <div className="flex items-center gap-1.5 text-xs text-gray-500">
              <GitBranch size={12} />
              <span className="font-mono">{activePipeline.name}</span>
            </div>
          )}
        </div>
      </header>

      {/* ── Page content ─────────────────────────────────────────────────── */}
      <main className="flex-1 overflow-hidden">
        <Outlet />
      </main>
    </div>
  );
}
