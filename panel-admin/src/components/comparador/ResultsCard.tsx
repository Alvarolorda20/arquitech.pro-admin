"use client";

import { motion } from "framer-motion";
import { useTranslations } from "next-intl";
import { CheckCircle2, Download } from "lucide-react";

interface ResultsCardProps {
  onDownload: () => void;
  filename: string;
}

export default function ResultsCard({ onDownload, filename }: ResultsCardProps) {
  const t = useTranslations('results');
  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ type: "spring", stiffness: 220, damping: 22 }}
      className="cmp-results-card flex flex-col items-center gap-6 rounded-2xl border border-teal-200 bg-gradient-to-b from-teal-50 to-white px-8 py-12 shadow-lg text-center"
    >
      <div className="cmp-results-icon flex h-16 w-16 items-center justify-center rounded-full bg-teal-600 shadow-md">
        <CheckCircle2 className="h-8 w-8 text-white" />
      </div>

      <div className="cmp-results-copy flex flex-col gap-1">
        <h3 className="cmp-results-title text-xl font-semibold text-gray-800">{t('title')}</h3>
        <p className="cmp-results-message text-sm text-gray-500">
          {t('readyMsg', {filename})}
        </p>
      </div>

      <button
        onClick={onDownload}
        className="
          cmp-results-download
          flex items-center gap-2 rounded-xl bg-teal-600 px-7 py-3
          text-sm font-semibold text-white shadow-sm
          hover:bg-teal-700 active:scale-[0.98] transition-all duration-150
        "
      >
        <Download className="h-4 w-4" />
        {t('download')}
      </button>
    </motion.div>
  );
}
