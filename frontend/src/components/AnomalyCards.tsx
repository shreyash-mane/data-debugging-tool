// AnomalyCards.tsx — Displays anomaly list for the selected step

import { AlertTriangle, AlertCircle, Info, ChevronDown, ChevronUp } from 'lucide-react';
import type { Anomaly } from '../types';
import { useState } from 'react';
import clsx from 'clsx';

interface Props {
  anomalies: Anomaly[];
}

const SEVERITY_CONFIG = {
  critical: {
    icon: AlertCircle,
    classes: 'bg-red-900/20 border-red-800/50 text-red-300',
    badge: 'badge-critical',
    iconColor: 'text-red-400',
    label: 'Critical',
  },
  warning: {
    icon: AlertTriangle,
    classes: 'bg-amber-900/15 border-amber-800/40 text-amber-300',
    badge: 'badge-warning',
    iconColor: 'text-amber-400',
    label: 'Warning',
  },
  info: {
    icon: Info,
    classes: 'bg-blue-900/15 border-blue-800/40 text-blue-300',
    badge: 'badge-info',
    iconColor: 'text-blue-400',
    label: 'Info',
  },
};

export default function AnomalyCards({ anomalies }: Props) {
  const [expanded, setExpanded] = useState<number | null>(0);

  if (anomalies.length === 0) {
    return (
      <div className="text-center py-6 text-gray-600 text-xs space-y-1">
        <div className="text-ok text-base">✓</div>
        <p>No anomalies detected at this step.</p>
      </div>
    );
  }

  const criticals = anomalies.filter(a => a.severity === 'critical').length;
  const warnings = anomalies.filter(a => a.severity === 'warning').length;

  return (
    <div className="space-y-2">
      {/* Summary bar */}
      <div className="flex items-center gap-2 text-xs mb-3">
        <span className="text-gray-400">
          {anomalies.length} issue{anomalies.length !== 1 ? 's' : ''}
        </span>
        {criticals > 0 && <span className="badge-critical">{criticals} critical</span>}
        {warnings > 0 && <span className="badge-warning">{warnings} warning</span>}
      </div>

      {anomalies.map((a, i) => {
        const cfg = SEVERITY_CONFIG[a.severity] ?? SEVERITY_CONFIG.info;
        const Icon = cfg.icon;
        const isOpen = expanded === i;
        return (
          <div
            key={i}
            className={clsx('rounded-lg border text-xs cursor-pointer transition-all', cfg.classes)}
            onClick={() => setExpanded(isOpen ? null : i)}
          >
            <div className="flex items-start gap-2 px-3 py-2.5">
              <Icon size={13} className={clsx('mt-0.5 flex-shrink-0', cfg.iconColor)} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className={cfg.badge}>{cfg.label}</span>
                  {a.column && (
                    <span className="font-mono text-[10px] text-gray-400">{a.column}</span>
                  )}
                </div>
                <p className={clsx('leading-relaxed', !isOpen && 'line-clamp-2')}>
                  {a.message}
                </p>
                {a.value !== null && a.value !== undefined && (
                  <div className="mt-1 font-mono text-[10px] opacity-70">
                    value: {String(a.value)}
                    {a.threshold !== null && ` (threshold: ${a.threshold})`}
                  </div>
                )}
              </div>
              {isOpen ? <ChevronUp size={11} className="flex-shrink-0 opacity-50 mt-1" /> : <ChevronDown size={11} className="flex-shrink-0 opacity-50 mt-1" />}
            </div>
          </div>
        );
      })}
    </div>
  );
}
