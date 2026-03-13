"use client";

import { motion } from "framer-motion";
import { useTranslations } from "next-intl";
import { Loader2, ScanSearch } from "lucide-react";

interface ProcessingOverlayProps {
  progress: number;  // 0–100
  message: string;   // live message from the backend
}

export default function ProcessingOverlay({ progress, message }: ProcessingOverlayProps) {
  const t = useTranslations('processing');
  const safe = Math.min(Math.max(progress, 0), 100);

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.97 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.97 }}
      transition={{ duration: 0.3 }}
      className="cmp-processing-card relative isolate mx-auto flex w-full max-w-[540px] flex-col items-center gap-6 overflow-hidden rounded-3xl border px-8 py-10 text-center"
    >
      <span className="cmp-processing-glow cmp-processing-glow--top" aria-hidden="true" />
      <span className="cmp-processing-glow cmp-processing-glow--bottom" aria-hidden="true" />

      {/* Animated icon */}
      <div className="cmp-processing-icon-wrap relative flex items-center justify-center">
        <span className="cmp-processing-ping absolute h-20 w-20 rounded-full" />
        <span className="cmp-processing-ring absolute h-24 w-24 rounded-full" aria-hidden="true" />
        <div className="cmp-processing-icon relative flex h-16 w-16 items-center justify-center rounded-full">
          <ScanSearch className="h-7 w-7 text-white" />
        </div>
      </div>

      {/* Title + live message */}
      <div className="cmp-processing-copy flex w-full flex-col gap-1">
        <p className="cmp-processing-title text-lg font-semibold">
          {t('title')}
        </p>
        <motion.p
          key={message}
          initial={{ opacity: 0, y: 6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.35 }}
          className="cmp-processing-message min-h-[1.25rem] text-sm"
        >
          {message || t('initializing')}
        </motion.p>
      </div>

      {/* Progress bar */}
      <div className="cmp-processing-progress-wrap flex w-full flex-col gap-2">
        <div className="cmp-processing-track w-full overflow-hidden rounded-full">
          <motion.div
            className="cmp-processing-bar h-full rounded-full"
            initial={{ width: "0%" }}
            animate={{ width: `${safe}%` }}
            transition={{ duration: 0.7, ease: "easeOut" }}
          />
          <span className="cmp-processing-shimmer" aria-hidden="true" />
        </div>
        <span className="cmp-processing-progress text-right text-xs">{safe}%</span>
      </div>

      {/* Footer */}
      <div className="cmp-processing-footer flex items-center gap-2 text-xs">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        {t('footer')}
      </div>
    </motion.div>
  );
}
