// ExplanationPanel.tsx — Displays root cause explanations with confidence

import { Lightbulb, CheckCircle2, Wrench, Search, ChevronDown, ChevronUp } from 'lucide-react';
import type { Explanation } from '../types';
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

export default function ExplanationPanel({ explanations }: Props) {
  const [expanded, setExpanded] = useState<number | null>(0);

  if (explanations.length === 0) {
    return (
      <div className="text-center py-6 text-gray-600 text-xs">
        No explanations — no anomalies detected.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {explanations.map((exp, i) => {
        const isOpen = expanded === i;
        return (
          <div
            key={i}
            className="card border-surface-3 rounded-lg overflow-hidden cursor-pointer"
            onClick={() => setExpanded(isOpen ? null : i)}
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
                {/* Likely cause */}
                <div className="flex items-start gap-2">
                  <CheckCircle2 size={12} className="text-accent mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-gray-400 text-[10px] uppercase tracking-wider mb-0.5">Likely Cause</p>
                    <p className="text-gray-200 leading-relaxed">{exp.likely_cause}</p>
                  </div>
                </div>

                {/* Recommended checks */}
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

                {/* Suggested fix */}
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
