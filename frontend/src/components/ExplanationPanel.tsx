// ExplanationPanel.tsx — Displays root cause explanations with confidence

import { Lightbulb, CheckCircle2, Wrench, Search, ChevronDown, ChevronUp, Sparkles, Tag, AlertTriangle } from 'lucide-react';
import type { Explanation, AutoCleanIssue } from '../types';
import { useState } from 'react';
import clsx from 'clsx';

interface Props {
  explanations: Explanation[];
}

const CONFIDENCE_STYLE = {
  high: 'text-ok border-ok/30 bg-ok/10',
  medium: 'text-amber-400 border-amber-600/30 bg-amber-900/10',
  low: 'text-gray-400 border-gray-600/30 bg-gray-900/10',
};

const SEVERITY_ICON_COLOR = {
  critical: 'text-red-400',
  warning: 'text-amber-400',
  info: 'text-blue-400',
};

// ── Auto-clean card ────────────────────────────────────────────────────────────

const TYPE_BADGE: Record<string, string> = {
  numeric: 'bg-blue-900/30 text-blue-400 border-blue-800/40',
  categorical: 'bg-purple-900/30 text-purple-400 border-purple-800/40',
  datetime: 'bg-teal-900/30 text-teal-400 border-teal-800/40',
  text: 'bg-gray-800/50 text-gray-400 border-gray-700/40',
};

const ISSUE_SEVERITY: Record<string, string> = {
  critical: 'text-red-400',
  warning: 'text-amber-400',
  info: 'text-blue-400',
};

function AutoCleanCard({ exp, isOpen, onToggle }: { exp: Explanation; isOpen: boolean; onToggle: () => void }) {
  const imp = exp.imputation;
  const isDropped = imp?.decision === 'DROP_COLUMN';

  return (
    <div
      className={clsx(
        'rounded-lg border overflow-hidden cursor-pointer transition-all',
        isDropped ? 'border-amber-800/40 bg-amber-900/5' : 'border-surface-3'
      )}
      onClick={onToggle}
    >
      {/* Header */}
      <div className="px-3 py-2 flex items-center gap-2 bg-surface-2">
        <Sparkles size={12} className={clsx('flex-shrink-0', isDropped ? 'text-amber-400' : 'text-ok')} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-xs font-medium text-gray-200 font-mono">{exp.column}</span>
            {exp.detected_type && (
              <span className={clsx('text-[9px] px-1 py-0.5 rounded border font-semibold uppercase tracking-wide', TYPE_BADGE[exp.detected_type] ?? TYPE_BADGE.text)}>
                {exp.detected_type}
              </span>
            )}
            {isDropped && (
              <span className="text-[9px] px-1 py-0.5 rounded border bg-amber-900/30 text-amber-400 border-amber-800/40 font-semibold">
                DROPPED
              </span>
            )}
          </div>
          {imp && imp.decision !== 'none' && imp.decision !== 'DROP_COLUMN' && (
            <p className="text-[10px] text-gray-500 mt-0.5 font-mono">
              {imp.missing_count} missing ({imp.missing_pct}%) → {imp.decision}
              {imp.fill_value !== undefined ? ` = ${typeof imp.fill_value === 'number' ? Number(imp.fill_value).toFixed(4) : imp.fill_value}` : ''}
            </p>
          )}
          {!imp && (exp.cleaning_steps ?? []).length > 0 && (
            <p className="text-[10px] text-ok/70 mt-0.5">{exp.cleaning_steps!.length} formatting fix(es)</p>
          )}
          {!imp && (exp.cleaning_steps ?? []).length === 0 && (
            <p className="text-[10px] text-gray-600 mt-0.5">No changes needed</p>
          )}
        </div>
        {isOpen ? <ChevronUp size={11} className="text-gray-600 flex-shrink-0" /> : <ChevronDown size={11} className="text-gray-600 flex-shrink-0" />}
      </div>

      {/* Expanded body */}
      {isOpen && (
        <div className="px-3 py-2.5 space-y-2.5 text-xs border-t border-surface-3">
          {/* Issues detected */}
          {(exp.issues_found ?? []).length > 0 && (
            <div>
              <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5 flex items-center gap-1">
                <AlertTriangle size={10} className="text-amber-400" /> Issues detected
              </p>
              <div className="space-y-1">
                {exp.issues_found!.map((issue, ii) => (
                  <div key={ii} className="flex items-start gap-1.5">
                    <span className={clsx('mt-0.5 text-[10px]', ISSUE_SEVERITY[issue.severity] ?? 'text-gray-400')}>•</span>
                    <span className="text-gray-300">{issue.message}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Cleaning steps */}
          {(exp.cleaning_steps ?? []).length > 0 && (
            <div>
              <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">Formatting fixes applied</p>
              <div className="space-y-0.5">
                {exp.cleaning_steps!.map((step, si) => (
                  <p key={si} className="text-ok/80 flex items-start gap-1.5">
                    <span className="mt-0.5">✓</span> {step}
                  </p>
                ))}
              </div>
            </div>
          )}

          {/* Imputation decision */}
          {imp && imp.decision !== 'none' && (
            <div className="bg-surface-2 rounded-lg p-2.5 space-y-1">
              <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">Missing value decision</p>
              <div className="flex items-baseline gap-2">
                <span className="font-semibold text-white">{imp.decision}</span>
                {imp.fill_value !== undefined && (
                  <span className="text-gray-400 font-mono text-[11px]">
                    fill = {typeof imp.fill_value === 'number' ? Number(imp.fill_value).toFixed(4) : `"${imp.fill_value}"`}
                  </span>
                )}
              </div>
              <p className="text-gray-300 leading-relaxed">{imp.reason}</p>
              {imp.skewness !== undefined && (
                <p className="text-[10px] text-gray-500 font-mono">skewness = {imp.skewness}</p>
              )}
            </div>
          )}

          {/* Recommended checks */}
          {(exp.recommended_checks ?? []).length > 0 && (
            <div className="space-y-0.5">
              {exp.recommended_checks.map((check, ci) => (
                <p key={ci} className="text-amber-400/80 flex items-start gap-1.5 text-[11px]">
                  <span className="mt-0.5">→</span> {check}
                </p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}


// ── Main panel ─────────────────────────────────────────────────────────────────

export default function ExplanationPanel({ explanations }: Props) {
  const [expanded, setExpanded] = useState<number | null>(0);

  if (explanations.length === 0) {
    return (
      <div className="text-center py-6 text-gray-600 text-xs">
        No explanations — no anomalies detected.
      </div>
    );
  }

  const autoCleanExps = explanations.filter(e => e.anomaly_type === 'auto_clean' || e.anomaly_type?.startsWith('global_'));
  const standardExps = explanations.filter(e => e.anomaly_type !== 'auto_clean' && !e.anomaly_type?.startsWith('global_'));
  const isAutoCleanStep = autoCleanExps.length > 0;

  return (
    <div className="space-y-2">
      {/* Auto-clean column cards */}
      {isAutoCleanStep && (
        <div className="space-y-1.5">
          <p className="text-[10px] text-ok font-semibold uppercase tracking-wider flex items-center gap-1.5 mb-2">
            <Sparkles size={10} /> Auto-Clean Decision Report
          </p>
          {autoCleanExps.map((exp, i) => (
            <AutoCleanCard
              key={i}
              exp={exp}
              isOpen={expanded === i}
              onToggle={() => setExpanded(expanded === i ? null : i)}
            />
          ))}
        </div>
      )}

      {/* Standard anomaly cards */}
      {standardExps.map((exp, i) => {
        const idx = autoCleanExps.length + i;
        const isOpen = expanded === idx;
        return (
          <div
            key={idx}
            className="card border-surface-3 rounded-lg overflow-hidden cursor-pointer"
            onClick={() => setExpanded(isOpen ? null : idx)}
          >
            {/* Header */}
            <div className="px-3 py-2.5 flex items-center gap-2 bg-surface-2">
              <Lightbulb
                size={13}
                className={clsx('flex-shrink-0', SEVERITY_ICON_COLOR[exp.severity] ?? 'text-gray-400')}
              />
              <div className="flex-1 min-w-0">
                <p className="text-xs font-medium text-gray-200 truncate">
                  {exp.summary}
                </p>
                {exp.column && (
                  <p className="text-[10px] font-mono text-gray-500 mt-0.5">{exp.column}</p>
                )}
              </div>
              <span className={clsx(
                'badge text-[9px] border flex-shrink-0',
                CONFIDENCE_STYLE[exp.confidence]
              )}>
                {exp.confidence} confidence
              </span>
              {isOpen
                ? <ChevronUp size={11} className="text-gray-600 flex-shrink-0" />
                : <ChevronDown size={11} className="text-gray-600 flex-shrink-0" />}
            </div>

            {/* Body */}
            {isOpen && (
              <div className="px-3 py-3 space-y-3 text-xs border-t border-surface-3">
                <div className="flex items-start gap-2">
                  <CheckCircle2 size={12} className="text-accent mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-gray-400 text-[10px] uppercase tracking-wider mb-0.5">Likely Cause</p>
                    <p className="text-gray-200 leading-relaxed">{exp.likely_cause}</p>
                  </div>
                </div>

                {exp.recommended_checks.length > 0 && (
                  <div className="flex items-start gap-2">
                    <Search size={12} className="text-amber-400 mt-0.5 flex-shrink-0" />
                    <div>
                      <p className="text-gray-400 text-[10px] uppercase tracking-wider mb-1">Recommended Checks</p>
                      <ul className="space-y-0.5">
                        {exp.recommended_checks.map((check, ci) => (
                          <li key={ci} className="text-gray-300 flex items-start gap-1.5">
                            <span className="text-gray-600 mt-0.5">•</span>
                            {check}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                <div className="flex items-start gap-2 bg-surface-2 rounded-lg p-2.5">
                  <Wrench size={12} className="text-ok mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-gray-400 text-[10px] uppercase tracking-wider mb-0.5">Suggested Fix</p>
                    <p className="text-gray-200 leading-relaxed">{exp.suggested_fix}</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
