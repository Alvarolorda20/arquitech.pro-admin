"use client";

import { useRef, useState, useCallback } from "react";
import { useTranslations } from "next-intl";
import { motion, AnimatePresence } from "framer-motion";
import { FileSpreadsheet, CheckCircle2, UploadCloud, X } from "lucide-react";

interface ExcelUploaderProps {
  file: File | null;
  onChange: (file: File | null) => void;
  disabled?: boolean;
}

export default function ExcelUploader({ file, onChange, disabled }: ExcelUploaderProps) {
  const t = useTranslations('excel');
  const inputRef = useRef<HTMLInputElement>(null);
  const [isDragging, setIsDragging] = useState(false);

  const handleFile = useCallback(
    (incoming: File | null) => {
      if (!incoming) return;
      if (!incoming.name.match(/\.(xlsx|xls)$/i)) return;
      onChange(incoming);
    },
    [onChange]
  );

  const onDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      setIsDragging(false);
      const dropped = e.dataTransfer.files?.[0] ?? null;
      handleFile(dropped);
    },
    [handleFile]
  );

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
      onDragLeave={() => setIsDragging(false)}
      onDrop={onDrop}
      onClick={() => !disabled && inputRef.current?.click()}
      className={`
        cmp-upload-dropzone cmp-upload-dropzone--excel
        relative flex flex-col items-center justify-center gap-3
        rounded-xl border-2 border-dashed px-6 py-10 text-center
        transition-all duration-200 cursor-pointer select-none
        ${disabled ? "opacity-50 cursor-not-allowed" : ""}
        ${isDragging ? "border-teal-500 bg-teal-50" : file ? "border-teal-400 bg-teal-50/50" : "border-gray-200 bg-gray-50 hover:border-teal-400 hover:bg-teal-50/30"}
      `}
    >
      <input
        ref={inputRef}
        type="file"
        accept=".xlsx,.xls"
        className="hidden"
        disabled={disabled}
        onChange={(e) => handleFile(e.target.files?.[0] ?? null)}
      />

      <AnimatePresence mode="wait">
        {file ? (
          <motion.div
            key="file-ready"
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.8 }}
            transition={{ type: "spring", stiffness: 300, damping: 24 }}
            className="cmp-upload-ready flex flex-col items-center gap-2"
          >
            <div className="cmp-upload-file-pill flex items-center gap-3 rounded-lg bg-white px-4 py-3 shadow-sm border border-teal-200">
              <FileSpreadsheet className="h-6 w-6 text-teal-600 shrink-0" />
              <span className="cmp-upload-file-name text-sm font-medium text-gray-700 truncate max-w-[220px]">{file.name}</span>
              <CheckCircle2 className="h-5 w-5 text-teal-500 shrink-0" />
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); onChange(null); if (inputRef.current) inputRef.current.value = ""; }}
                className="cmp-upload-remove ml-1 text-gray-400 hover:text-red-500 transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <p className="cmp-upload-ready-label text-xs text-teal-600 font-medium">{t('ready')}</p>
          </motion.div>
        ) : (
          <motion.div
            key="upload-prompt"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="cmp-upload-prompt flex flex-col items-center gap-2"
          >
            <UploadCloud className={`cmp-upload-icon h-10 w-10 transition-colors ${isDragging ? "text-teal-500" : "text-gray-300"}`} />
            <p className="cmp-upload-main-text text-sm font-medium text-gray-600">
              {t('dragPrompt')}{" "}
              <span className="cmp-upload-action text-teal-600 underline">{t('select')}</span>
            </p>
            <p className="cmp-upload-subtext text-xs text-gray-400">{t('formats')}</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
