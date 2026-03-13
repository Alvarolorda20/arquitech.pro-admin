"use client";

import { useRef, useState, useCallback } from "react";
import { useTranslations } from "next-intl";
import { motion, AnimatePresence } from "framer-motion";
import { FileText, UploadCloud, X } from "lucide-react";

interface PdfUploaderProps {
  files: File[];
  onChange: (files: File[]) => void;
  disabled?: boolean;
}

export default function PdfUploader({ files, onChange, disabled }: PdfUploaderProps) {
  const t = useTranslations('pdf');
  const inputRef = useRef<HTMLInputElement>(null);
  const [isDragging, setIsDragging] = useState(false);

  const addFiles = useCallback(
    (incoming: FileList | null) => {
      if (!incoming) return;
      const pdfs = Array.from(incoming).filter((f) => f.name.match(/\.pdf$/i));
      if (pdfs.length === 0) return;
      // Deduplicate by name
      const existing = new Set(files.map((f) => f.name));
      const merged = [...files, ...pdfs.filter((f) => !existing.has(f.name))];
      onChange(merged);
    },
    [files, onChange]
  );

  const removeFile = useCallback(
    (name: string) => {
      onChange(files.filter((f) => f.name !== name));
    },
    [files, onChange]
  );

  const onDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      setIsDragging(false);
      addFiles(e.dataTransfer.files);
    },
    [addFiles]
  );

  return (
    <div className="flex flex-col gap-3">
      <div
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={onDrop}
        onClick={() => !disabled && inputRef.current?.click()}
        className={`
          cmp-upload-dropzone cmp-upload-dropzone--pdf
          relative flex flex-col items-center justify-center gap-3
          rounded-xl border-2 border-dashed px-6 py-12 text-center
          transition-all duration-200 cursor-pointer select-none
          ${disabled ? "opacity-50 cursor-not-allowed" : ""}
          ${isDragging ? "border-amber-500 bg-amber-50" : "border-gray-200 bg-gray-50 hover:border-amber-400 hover:bg-amber-50/30"}
        `}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".pdf"
          multiple
          className="hidden"
          disabled={disabled}
          onChange={(e) => { addFiles(e.target.files); if (inputRef.current) inputRef.current.value = ""; }}
        />
        <UploadCloud className={`cmp-upload-icon h-12 w-12 transition-colors ${isDragging ? "text-amber-500" : "text-gray-300"}`} />
        <div>
          <p className="cmp-upload-main-text text-sm font-medium text-gray-600">
            {t('dragPrompt')}{" "}
            <span className="cmp-upload-action text-amber-600 underline">{t('select')}</span>
          </p>
          <p className="cmp-upload-subtext text-xs text-gray-400 mt-1">{t('hint')}</p>
        </div>
      </div>

      {/* File chip list */}
      <AnimatePresence>
        {files.length > 0 && (
          <motion.ul
            initial={{ opacity: 0, y: -4 }}
            animate={{ opacity: 1, y: 0 }}
            className="flex flex-col gap-2"
          >
            {files.map((file) => (
              <motion.li
                key={file.name}
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 8 }}
                transition={{ type: "spring", stiffness: 300, damping: 24 }}
                className="cmp-upload-file-pill cmp-upload-file-pill--pdf flex items-center gap-3 rounded-lg border border-amber-200 bg-amber-50/60 px-4 py-2.5"
              >
                <FileText className="h-4 w-4 text-amber-600 shrink-0" />
                <span className="cmp-upload-file-name flex-1 text-sm font-medium text-gray-700 truncate">{file.name}</span>
                <span className="cmp-upload-subtext text-xs text-gray-400 shrink-0">
                  {(file.size / 1024 / 1024).toFixed(1)} MB
                </span>
                {!disabled && (
                  <button
                    type="button"
                    onClick={() => removeFile(file.name)}
                    className="cmp-upload-remove text-gray-400 hover:text-red-500 transition-colors"
                  >
                    <X className="h-4 w-4" />
                  </button>
                )}
              </motion.li>
            ))}
          </motion.ul>
        )}
      </AnimatePresence>
    </div>
  );
}
