"use client";

import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {useTranslations} from 'next-intl';

import axios, {type AxiosError} from 'axios';
import {AnimatePresence, motion, type Variants} from 'framer-motion';
import {Button, Text} from '@gravity-ui/uikit';
import {Download, FileSpreadsheet, FileText, Files, LayoutPanelTop} from 'lucide-react';

import ExcelUploader from '@/components/comparador/ExcelUploader';
import PdfUploader from '@/components/comparador/PdfUploader';
import ProcessingOverlay from '@/components/comparador/ProcessingOverlay';
import {getPublicBackendApiBaseUrl} from '@/lib/backend-api';
import {createSupabaseBrowserClient} from '@/lib/supabase/client';

//  Types 
type AppState = 'idle' | 'processing' | 'error' | 'summary';

type SummaryFileType = 'output' | 'pauta' | 'pdf';

interface SummaryFileItem {
  id: string;
  name: string;
  type: SummaryFileType;
  downloadHref: string;
}

interface CompletedRunSnapshot {
  runId: string | null;
  jobId: string;
  startedAt: string | null;
  finishedAt: string | null;
  pautaFilename: string | null;
  pdfFilenames: string[];
}

interface CreditEstimatePayload {
  estimate?: {
    final_credits?: number;
    base_credits?: number;
    pdf_credits?: number;
    size_credits?: number;
    margin_percent?: number;
    total_megabytes?: number;
    size_mode?: string;
  } | null;
}

interface CreditEstimateState {
  loading: boolean;
  finalCredits: number | null;
  baseCredits: number;
  pdfCredits: number;
  sizeCredits: number;
  marginPercent: number;
  totalMegabytes: number;
  sizeMode: string;
}

type BillingKind = 'none' | 'credits' | 'quota' | 'unknown';

//  Constants 
const POLL_MS = 3_000;

function buildApiUrl(baseUrl: string, path: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return baseUrl ? `${baseUrl}${normalizedPath}` : normalizedPath;
}

//  Animation variants 
const sectionVariants: Variants = {
  hidden: {opacity: 0, y: 16},
  visible: (i: number) => ({
    opacity: 1,
    y: 0,
    transition: {delay: i * 0.12, type: 'spring' as const, stiffness: 220, damping: 22},
  }),
};

//  Sub-components 
function StepBadge({number, label}: {number: number; label: string}) {
  return (
    <div className="presup-step-badge">
      <span className="presup-step-number">{number}</span>
      <Text as="span" variant="subheader-2">
        {label}
      </Text>
    </div>
  );
}

function SectionCard({children, className = ''}: {children: React.ReactNode; className?: string}) {
  return <div className={`presup-section-card ${className}`}>{children}</div>;
}

function ReadinessRow({label, ready}: {label: string; ready: boolean}) {
  return (
    <div className="presup-readiness-row">
      <span className={`presup-readiness-dot ${ready ? 'presup-readiness-dot--ready' : ''}`} />
      <Text as="span" variant="body-1" color={ready ? undefined : 'secondary'}>
        {label}
      </Text>
    </div>
  );
}

//  Main component 
interface ComparadorContentProps {
  projectId?: string | null;
  tenantId?: string | null;
  projectName?: string | null;
  readOnly?: boolean;
  initialJobId?: string | null;
  lastFailedRunId?: string | null;
  lastFailedPautaFilename?: string | null;
  lastFailedPdfFilenames?: string[];
  latestCompletedRun?: CompletedRunSnapshot | null;
}

function buildOutputDisplayFilename(projectName?: string | null): string {
  const baseName = (projectName ?? '').trim();
  if (!baseName) return 'comparativo.xlsx';
  return `${baseName}_comparativo.xlsx`;
}

function toValidDate(value: string | null | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function formatDuration(startedAt: string | null | undefined, finishedAt: string | null | undefined): string | null {
  const start = toValidDate(startedAt);
  const end = toValidDate(finishedAt);
  if (!start || !end) return null;
  const diffMs = end.getTime() - start.getTime();
  if (diffMs < 0) return null;

  const totalSeconds = Math.round(diffMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function fileExtension(filename: string): string {
  const parts = filename.split('.');
  if (parts.length < 2) return 'FILE';
  return String(parts[parts.length - 1] || 'file').toUpperCase();
}

function normalizeFilenameList(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((name) => String(name || '').trim()).filter((name) => name.length > 0);
}

export default function ComparadorContent({
  projectId = null,
  tenantId = null,
  projectName = null,
  readOnly = false,
  initialJobId = null,
  lastFailedRunId = null,
  lastFailedPautaFilename = null,
  lastFailedPdfFilenames = [],
  latestCompletedRun = null,
}: ComparadorContentProps) {
  const t = useTranslations('comparador');
  const supabase = useMemo(() => createSupabaseBrowserClient(), []);
  const backendBaseUrl = useMemo(() => getPublicBackendApiBaseUrl(), []);
  const initialResumeJobId = String(initialJobId || '').trim() || null;
  const initialCompletedRun =
    latestCompletedRun && String(latestCompletedRun.jobId || '').trim()
      ? latestCompletedRun
      : null;
  const [completedRun, setCompletedRun] = useState<CompletedRunSnapshot | null>(initialCompletedRun);

  const [pautaFile, setPautaFile] = useState<File | null>(null);
  const [ofertaFiles, setOfertaFiles] = useState<File[]>([]);
  const [appState, setAppState] = useState<AppState>(() => {
    if (initialResumeJobId) return 'processing';
    if (initialCompletedRun) return 'summary';
    return 'idle';
  });
  const [errorMessage, setErrorMessage] = useState<string>('');
  const [progress, setProgress] = useState(0);
  const [statusMsg, setStatusMsg] = useState(initialResumeJobId ? t('processing') : '');
  const [rerunInProgress, setRerunInProgress] = useState(false);
  const [rerunOverrideFiles, setRerunOverrideFiles] = useState<File[]>([]);
  const [rerunOverrideInProgress, setRerunOverrideInProgress] = useState(false);
  const [showRerunConfigurator, setShowRerunConfigurator] = useState(false);
  const [rerunAuditSelection, setRerunAuditSelection] = useState<Record<string, boolean>>({});
  const [creditEstimate, setCreditEstimate] = useState<CreditEstimateState>({
    loading: false,
    finalCredits: null,
    baseCredits: 0,
    pdfCredits: 0,
    sizeCredits: 0,
    marginPercent: 0,
    totalMegabytes: 0,
    sizeMode: '',
  });
  const [billingKind, setBillingKind] = useState<BillingKind>('unknown');
  const [billingKindResolved, setBillingKindResolved] = useState(false);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollInFlightRef = useRef(false);
  const pollErrorCountRef = useRef(0);
  const resumedJobRef = useRef<string | null>(null);
  const runStartedAtRef = useRef<string | null>(initialCompletedRun?.startedAt || null);

  const hasPersistenceContext = Boolean(projectId && tenantId);
  const canProcess = !readOnly && pautaFile !== null && ofertaFiles.length > 0 && hasPersistenceContext;
  const totalUploadBytes = useMemo(
    () => (pautaFile?.size || 0) + ofertaFiles.reduce((acc, file) => acc + (file.size || 0), 0),
    [ofertaFiles, pautaFile],
  );
  const fileSizesBytes = useMemo(
    () => [
      ...(pautaFile ? [Math.max(0, pautaFile.size || 0)] : []),
      ...ofertaFiles.map((file) => Math.max(0, file.size || 0)),
    ],
    [ofertaFiles, pautaFile],
  );
  const isProcessing = appState === 'processing';
  const outputDisplayFilename = buildOutputDisplayFilename(projectName);
  const completedRunDuration = formatDuration(
    completedRun?.startedAt,
    completedRun?.finishedAt,
  );
  const completedRunStartedAt = toValidDate(completedRun?.startedAt)?.toLocaleString() || null;
  const completedRunFinishedAt = toValidDate(completedRun?.finishedAt)?.toLocaleString() || null;
  const summaryFiles = useMemo<SummaryFileItem[]>(() => {
    if (!completedRun) return [];

    const safeJobId = String(completedRun.jobId || '').trim();
    if (!safeJobId) return [];

    const files: SummaryFileItem[] = [];

    files.push({
      id: 'output',
      name: outputDisplayFilename,
      type: 'output',
      downloadHref: buildApiUrl(backendBaseUrl, `/api/download/${encodeURIComponent(safeJobId)}`),
    });

    if (completedRun.pautaFilename) {
      files.push({
        id: 'pauta',
        name: completedRun.pautaFilename,
        type: 'pauta',
        downloadHref: buildApiUrl(
          backendBaseUrl,
          `/api/download-input/${encodeURIComponent(safeJobId)}?kind=pauta&filename=${encodeURIComponent(completedRun.pautaFilename)}`,
        ),
      });
    }

    completedRun.pdfFilenames.forEach((name, index) => {
      files.push({
        id: `pdf-${index}`,
        name,
        type: 'pdf',
        downloadHref: buildApiUrl(
          backendBaseUrl,
          `/api/download-input/${encodeURIComponent(safeJobId)}?kind=pdf&filename=${encodeURIComponent(name)}`,
        ),
      });
    });

    return files;
  }, [backendBaseUrl, completedRun, outputDisplayFilename]);
  const existingRunPdfNames = useMemo(
    () => (completedRun?.pdfFilenames || []).map((name) => String(name || '').trim()).filter((name) => name.length > 0),
    [completedRun?.pdfFilenames],
  );
  const shouldShowCreditEstimate = billingKindResolved && billingKind === 'credits';

  useEffect(() => {
    if (!completedRun) return;
    const nextSelection: Record<string, boolean> = {};
    for (const name of existingRunPdfNames) {
      nextSelection[name] = true;
    }
    setRerunAuditSelection(nextSelection);
  }, [completedRun, existingRunPdfNames]);

  useEffect(() => () => {
    if (pollRef.current) clearTimeout(pollRef.current);
    pollInFlightRef.current = false;
    pollErrorCountRef.current = 0;
  }, []);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearTimeout(pollRef.current);
      pollRef.current = null;
    }
    pollInFlightRef.current = false;
    pollErrorCountRef.current = 0;
  }, []);

  const loadCompletedRunSnapshot = useCallback(
    async (targetJobId: string): Promise<CompletedRunSnapshot> => {
      const fallback: CompletedRunSnapshot = {
        runId: null,
        jobId: targetJobId,
        startedAt: runStartedAtRef.current,
        finishedAt: new Date().toISOString(),
        pautaFilename: pautaFile?.name || null,
        pdfFilenames: ofertaFiles.map((file) => file.name).filter((name) => name.trim().length > 0),
      };
      if (!tenantId || !projectId) {
        return fallback;
      }
      try {
        const {data, error} = await supabase
          .from('budget_runs')
          .select('id,started_at,finished_at,request_payload')
          .eq('tenant_id', tenantId)
          .eq('project_id', projectId)
          .eq('pipeline_job_id', targetJobId)
          .order('started_at', {ascending: false})
          .limit(1);
        if (error || !data || data.length === 0) {
          return fallback;
        }
        const row = data[0] as {
          id?: string | null;
          started_at?: string | null;
          finished_at?: string | null;
          request_payload?: {pauta_filename?: string; pdf_filenames?: unknown} | null;
        };
        const payload = row.request_payload && typeof row.request_payload === 'object'
          ? row.request_payload
          : null;
        return {
          runId: row.id ? String(row.id).trim() : fallback.runId,
          jobId: targetJobId,
          startedAt: row.started_at || fallback.startedAt,
          finishedAt: row.finished_at || fallback.finishedAt,
          pautaFilename: payload?.pauta_filename || fallback.pautaFilename,
          pdfFilenames: normalizeFilenameList(payload?.pdf_filenames).length > 0
            ? normalizeFilenameList(payload?.pdf_filenames)
            : fallback.pdfFilenames,
        };
      } catch {
        return fallback;
      }
    },
    [ofertaFiles, pautaFile, projectId, supabase, tenantId],
  );

  const pollJobStatus = useCallback(
    async (targetJobId: string): Promise<boolean> => {
      try {
        const {
          data: {session},
        } = await supabase.auth.getSession();
        const accessToken = String(session?.access_token || '').trim();
        const {data} = await axios.get<{
          status: string;
          progress: number;
          message: string;
          error?: string | null;
        }>(buildApiUrl(backendBaseUrl, `/api/status/${encodeURIComponent(targetJobId)}`), {
          headers: accessToken ? {Authorization: `Bearer ${accessToken}`} : undefined,
          timeout: 20_000,
        });
        pollErrorCountRef.current = 0;

        if (resumedJobRef.current !== targetJobId) {
          return true;
        }

        const normalizedStatus = String(data.status || '').trim().toLowerCase();
        if (typeof data.progress === 'number') {
          const safeProgress = Math.min(Math.max(data.progress, 0), 100);
          setProgress((previous) => {
            if (normalizedStatus === 'processing' || normalizedStatus === 'running' || normalizedStatus === 'queued') {
              return Math.max(previous, safeProgress);
            }
            return safeProgress;
          });
        }
        if (typeof data.message === 'string' && data.message.trim()) {
          setStatusMsg(data.message);
        }
        if (normalizedStatus === 'completed') {
          setProgress(100);
          setStatusMsg(t('completed'));
          const snapshot = await loadCompletedRunSnapshot(targetJobId);
          setCompletedRun(snapshot);
          setAppState('summary');
          return true;
        }
        if (normalizedStatus === 'failed' || normalizedStatus === 'cancelled') {
          setErrorMessage(t('failed'));
          setAppState('error');
          return true;
        }
        if (normalizedStatus === 'processing' || normalizedStatus === 'running' || normalizedStatus === 'queued') {
          setAppState('processing');
        }
        return false;
      } catch {
        if (resumedJobRef.current !== targetJobId) {
          return true;
        }
        pollErrorCountRef.current += 1;
        if (pollErrorCountRef.current >= 4) {
          setErrorMessage(t('errNoBackend'));
          setAppState('error');
          return true;
        }
        return false;
      }
    },
    [backendBaseUrl, loadCompletedRunSnapshot, supabase, t],
  );

  const startPolling = useCallback(
    (targetJobId: string) => {
      stopPolling();
      resumedJobRef.current = targetJobId;
      pollErrorCountRef.current = 0;
      const pollOnce = async () => {
        if (resumedJobRef.current !== targetJobId) return;
        if (pollInFlightRef.current) return;
        pollInFlightRef.current = true;
        const shouldStop = await pollJobStatus(targetJobId);
        pollInFlightRef.current = false;
        if (shouldStop || resumedJobRef.current !== targetJobId) {
          stopPolling();
          return;
        }
        pollRef.current = setTimeout(() => {
          void pollOnce();
        }, POLL_MS);
      };
      void pollOnce();
    },
    [pollJobStatus, stopPolling],
  );

  useEffect(() => {
    const safeInitialJobId = String(initialJobId || '').trim();
    if (!safeInitialJobId) return;
    if (resumedJobRef.current === safeInitialJobId) return;
    resumedJobRef.current = safeInitialJobId;
    setCompletedRun(null);
    startPolling(safeInitialJobId);
  }, [initialJobId, startPolling]);

  useEffect(() => {
    if (initialResumeJobId) return;
    if (!projectId || !tenantId) return;
    let cancelled = false;
    const resumeLatest = async () => {
      try {
        const {data, error} = await supabase
          .from('budget_runs')
          .select('pipeline_job_id,status,started_at')
          .eq('tenant_id', tenantId)
          .eq('project_id', projectId)
          .in('status', ['running', 'queued'])
          .order('started_at', {ascending: false})
          .limit(1);
        if (cancelled || error || !data || data.length === 0) return;
        const pipelineJobId = String(data[0]?.pipeline_job_id || '').trim();
        if (!pipelineJobId) return;
        if (resumedJobRef.current === pipelineJobId) return;
        resumedJobRef.current = pipelineJobId;
        setAppState('processing');
        setStatusMsg(t('processing'));
        runStartedAtRef.current = String(data[0]?.started_at || '').trim() || null;
        setCompletedRun(null);
        startPolling(pipelineJobId);
      } catch {
        // Soft-fail: manual process start remains available.
      }
    };
    void resumeLatest();
    return () => {
      cancelled = true;
    };
  }, [initialResumeJobId, projectId, supabase, t, tenantId, startPolling]);

  useEffect(() => {
    let cancelled = false;

    async function loadBillingKind() {
      const normalizedTenantId = String(tenantId || '').trim();
      if (!backendBaseUrl || !normalizedTenantId) {
        if (!cancelled) {
          setBillingKind('unknown');
          setBillingKindResolved(false);
        }
        return;
      }

      if (!cancelled) {
        setBillingKindResolved(false);
      }

      try {
        const {
          data: {session},
        } = await supabase.auth.getSession();
        const accessToken = String(session?.access_token || '').trim();
        if (!accessToken) {
          if (!cancelled) {
            setBillingKind('unknown');
            setBillingKindResolved(true);
          }
          return;
        }

        const response = await fetch(
          `${buildApiUrl(backendBaseUrl, '/api/credits/balance')}?tenant_id=${encodeURIComponent(normalizedTenantId)}`,
          {
            method: 'GET',
            headers: {
              authorization: `Bearer ${accessToken}`,
            },
            cache: 'no-store',
          },
        );

        if (!response.ok) {
          if (!cancelled) {
            setBillingKind('unknown');
            setBillingKindResolved(true);
          }
          return;
        }

        const payload = (await response.json().catch(() => ({}))) as {billing_kind?: string | null};
        const rawKind = String(payload.billing_kind || '').trim().toLowerCase();
        const normalizedKind: BillingKind =
          rawKind === 'credits' ? 'credits' : rawKind === 'quota' ? 'quota' : rawKind === 'none' ? 'none' : 'unknown';

        if (!cancelled) {
          setBillingKind(normalizedKind);
          setBillingKindResolved(true);
        }
      } catch {
        if (!cancelled) {
          setBillingKind('unknown');
          setBillingKindResolved(true);
        }
      }
    }

    void loadBillingKind();
    return () => {
      cancelled = true;
    };
  }, [backendBaseUrl, supabase, tenantId]);

  useEffect(() => {
    let cancelled = false;
    async function loadEstimate() {
      const normalizedTenantId = String(tenantId || '').trim();
      if (
        !shouldShowCreditEstimate ||
        !backendBaseUrl ||
        !normalizedTenantId ||
        !pautaFile ||
        ofertaFiles.length <= 0
      ) {
        if (!cancelled) {
          setCreditEstimate((previous) => ({
            ...previous,
            loading: false,
            finalCredits: null,
          }));
        }
        return;
      }

      if (!cancelled) {
        setCreditEstimate((previous) => ({...previous, loading: true}));
      }

      try {
        const {
          data: {session},
        } = await supabase.auth.getSession();
        const accessToken = String(session?.access_token || '').trim();
        if (!accessToken) {
          if (!cancelled) setCreditEstimate((previous) => ({...previous, loading: false, finalCredits: null}));
          return;
        }

        const query = new URLSearchParams({
          tenant_id: normalizedTenantId,
          pdf_count: String(ofertaFiles.length),
          total_bytes: String(Math.max(0, totalUploadBytes)),
          is_rerun: 'false',
        });
        for (const fileSize of fileSizesBytes) {
          query.append('file_sizes_bytes', String(Math.max(0, fileSize)));
        }
        const response = await fetch(`${buildApiUrl(backendBaseUrl, '/api/credits/estimate')}?${query.toString()}`, {
          method: 'GET',
          headers: {
            authorization: `Bearer ${accessToken}`,
          },
          cache: 'no-store',
        });
        if (!response.ok) {
          if (!cancelled) setCreditEstimate((previous) => ({...previous, loading: false, finalCredits: null}));
          return;
        }

        const payload = (await response.json().catch(() => ({}))) as CreditEstimatePayload;
        const estimate = payload?.estimate || {};
        if (!cancelled) {
          setCreditEstimate({
            loading: false,
            finalCredits:
              typeof estimate.final_credits === 'number' ? estimate.final_credits : null,
            baseCredits: Number(estimate.base_credits || 0),
            pdfCredits: Number(estimate.pdf_credits || 0),
            sizeCredits: Number(estimate.size_credits || 0),
            marginPercent: Number(estimate.margin_percent || 0),
            totalMegabytes: Number(estimate.total_megabytes || 0),
            sizeMode: String(estimate.size_mode || '').trim(),
          });
        }
      } catch {
        if (!cancelled) setCreditEstimate((previous) => ({...previous, loading: false, finalCredits: null}));
      }
    }

    void loadEstimate();
    return () => {
      cancelled = true;
    };
  }, [
    backendBaseUrl,
    fileSizesBytes,
    ofertaFiles,
    pautaFile,
    shouldShowCreditEstimate,
    supabase,
    tenantId,
    totalUploadBytes,
  ]);

  const handleRerunWithOverrides = useCallback(async () => {
    if (readOnly) return;
    if (!completedRun?.runId || !projectId || !tenantId) return;
    if (isProcessing || rerunOverrideInProgress) return;

    const rerunPdfNames = existingRunPdfNames.filter((name) => rerunAuditSelection[name] !== false);
    const reusePdfNames = existingRunPdfNames.filter((name) => rerunAuditSelection[name] === false);
    if (rerunPdfNames.length === 0 && reusePdfNames.length === 0 && rerunOverrideFiles.length === 0) return;

    setRerunOverrideInProgress(true);
    setAppState('processing');
    setErrorMessage('');
    setProgress(0);
    setStatusMsg(t('sending'));
    runStartedAtRef.current = new Date().toISOString();

    try {
      const {
        data: {session},
      } = await supabase.auth.getSession();
      const accessToken = session?.access_token;
      if (!accessToken) {
        setErrorMessage(t('errSessionExpired'));
        setAppState('error');
        return;
      }

      const formData = new FormData();
      formData.append('run_id', completedRun.runId);
      formData.append('project_id', projectId);
      formData.append('tenant_id', tenantId);
      formData.append('force_rerun', 'true');
      formData.append('rerun_pdf_filenames_json', JSON.stringify(rerunPdfNames));
      formData.append('reuse_pdf_filenames_json', JSON.stringify(reusePdfNames));
      rerunOverrideFiles.forEach((file) => formData.append('files', file));

      const res = await axios.post<{job_id: string}>(
        buildApiUrl(backendBaseUrl, '/api/process-budget/rerun-with-overrides'),
        formData,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'multipart/form-data',
          },
          timeout: 30_000,
        },
      );
      const newJobId = String(res.data.job_id || '').trim();
      if (!newJobId) {
        throw new Error('missing_job_id');
      }
      setRerunOverrideFiles([]);
      setShowRerunConfigurator(false);
      resumedJobRef.current = newJobId;
      setStatusMsg(t('processing'));
      startPolling(newJobId);
    } catch (err) {
      const axiosErr = err as AxiosError;
      let msg = t('errNoBackend');
      if (axiosErr.response?.status === 422) {
        msg = t('errInvalidFiles');
      } else if (axiosErr.response?.status === 401 || axiosErr.response?.status === 403) {
        msg = t('errSessionExpired');
      }
      setErrorMessage(msg);
      setAppState('error');
    } finally {
      setRerunOverrideInProgress(false);
    }
  }, [
    backendBaseUrl,
    completedRun?.runId,
    existingRunPdfNames,
    isProcessing,
    projectId,
    rerunAuditSelection,
    rerunOverrideFiles,
    rerunOverrideInProgress,
    startPolling,
    supabase.auth,
    t,
    tenantId,
    readOnly,
  ]);

  const handleProcess = useCallback(async () => {
    if (readOnly) return;
    if (!canProcess) return;

    setAppState('processing');
    setErrorMessage('');
    setProgress(0);
    setStatusMsg(t('sending'));
    setCompletedRun(null);
    runStartedAtRef.current = new Date().toISOString();

    const formData = new FormData();
    formData.append('pauta', pautaFile!);
    ofertaFiles.forEach((f) => formData.append('files', f));
    if (projectId) {
      formData.append('project_id', projectId);
    }
    if (tenantId) {
      formData.append('tenant_id', tenantId);
    }

    let newJobId: string;
    try {
      const {
        data: {session},
      } = await supabase.auth.getSession();
      const accessToken = session?.access_token;
      if (!accessToken) {
        setErrorMessage(t('errSessionExpired'));
        setAppState('error');
        return;
      }

      const res = await axios.post<{job_id: string}>(
        buildApiUrl(backendBaseUrl, '/api/process-budget'),
        formData,
        {
        headers: {
          'Content-Type': 'multipart/form-data',
          Authorization: `Bearer ${accessToken}`,
        },
        timeout: 30_000,
      });
      newJobId = res.data.job_id;
      resumedJobRef.current = newJobId;
      setStatusMsg(t('processing'));
      startPolling(newJobId);
    } catch (err) {
      const axiosErr = err as AxiosError;
      let msg = t('errNoBackend');
      if (axiosErr.response?.status === 422) {
        msg = t('errInvalidFiles');
      } else if (axiosErr.response?.status === 401 || axiosErr.response?.status === 403) {
        msg = t('errSessionExpired');
      } else if (axiosErr.code === 'ERR_NETWORK') {
        msg = t('errNetwork');
      }
      setErrorMessage(msg);
      setAppState('error');
      return;
    }
  }, [backendBaseUrl, canProcess, pautaFile, ofertaFiles, projectId, tenantId, startPolling, supabase, t, readOnly]);

  const goToNewComparisonProject = useCallback(() => {
    window.location.href = '/products/comparacion-presupuestos';
  }, []);

  const offersLabel =
    ofertaFiles.length > 0
      ? t('readiness.offers', {count: ofertaFiles.length})
      : t('readiness.noOffers');

  const hasFailedRunRecovery =
    !readOnly &&
    Boolean(lastFailedRunId) &&
    Boolean(projectId) &&
    Boolean(tenantId) &&
    Boolean(lastFailedPautaFilename) &&
    Array.isArray(lastFailedPdfFilenames) &&
    lastFailedPdfFilenames.length > 0;

  const handleRerunLastFailed = useCallback(async () => {
    if (readOnly) return;
    if (!hasFailedRunRecovery || !lastFailedRunId || !projectId || !tenantId) return;
    if (isProcessing || rerunInProgress) return;

    setRerunInProgress(true);
    setAppState('processing');
    setErrorMessage('');
    setProgress(0);
    setStatusMsg(t('sending'));
    setCompletedRun(null);
    runStartedAtRef.current = new Date().toISOString();

    try {
      const {
        data: {session},
      } = await supabase.auth.getSession();
      const accessToken = session?.access_token;
      if (!accessToken) {
        setErrorMessage(t('errSessionExpired'));
        setAppState('error');
        return;
      }

      const res = await axios.post<{job_id: string}>(
        buildApiUrl(backendBaseUrl, '/api/process-budget/rerun'),
        {
          run_id: lastFailedRunId,
          project_id: projectId,
          tenant_id: tenantId,
          force_rerun: true,
        },
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
          },
          timeout: 30_000,
        },
      );
      const newJobId = String(res.data.job_id || '').trim();
      if (!newJobId) {
        throw new Error('missing_job_id');
      }
      resumedJobRef.current = newJobId;
      setStatusMsg(t('processing'));
      startPolling(newJobId);
    } catch (err) {
      const axiosErr = err as AxiosError;
      let msg = t('errNoBackend');
      if (axiosErr.response?.status === 401 || axiosErr.response?.status === 403) {
        msg = t('errSessionExpired');
      }
      setErrorMessage(msg);
      setAppState('error');
    } finally {
      setRerunInProgress(false);
    }
  }, [
    hasFailedRunRecovery,
    backendBaseUrl,
    isProcessing,
    lastFailedRunId,
    projectId,
    rerunInProgress,
    startPolling,
    supabase.auth,
    t,
    tenantId,
    readOnly,
  ]);

  return (
    <div className={`presup-content ${appState === 'summary' ? 'presup-content--summary' : ''}`}>
      {/* {showHero ? (
        <motion.div
          initial={{opacity: 0, y: -10}}
          animate={{opacity: 1, y: 0}}
          transition={{duration: 0.5}}
          className="presup-hero"
        >
          <Text as="h1" variant="display-1" className="presup-hero-title">
            {t('title')}
          </Text>
          <Text as="p" variant="body-2" color="secondary" className="presup-hero-subtitle">
            {t('subtitle')}
          </Text>
        </motion.div>
      ) : null} */}

      <AnimatePresence mode="wait">
        {appState === 'summary' && completedRun && (
          <motion.div key="summary" className="presup-success-wrap">
            <motion.div
              initial={{opacity: 0, y: 12}}
              animate={{opacity: 1, y: 0}}
              transition={{type: 'spring', stiffness: 220, damping: 22}}
            >
              <SectionCard className="cmp-summary-panel border-teal-200/70 bg-gradient-to-br from-teal-50/75 via-white to-amber-50/55 shadow-[0_20px_45px_rgba(15,42,66,0.08)]">
                <div className="mb-3 flex items-center gap-2">
                  <span className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-teal-200 bg-teal-50 text-teal-700">
                    <LayoutPanelTop className="h-4.5 w-4.5" />
                  </span>
                  <h2 className="cmp-summary-title m-0 text-[24px] font-bold leading-tight tracking-[-0.01em]">
                    {t('summary.title')}
                  </h2>
                </div>
                <Text as="p" variant="body-1" color="secondary" style={{marginBottom: 14}}>
                  {t('summary.subtitle')}
                </Text>

                <div className="mb-5 grid gap-3 sm:grid-cols-3">
                  <div className="cmp-summary-stat-card rounded-2xl border border-teal-100 bg-white/80 p-3 shadow-sm">
                    <p className="cmp-summary-stat-label text-xs font-semibold uppercase tracking-wide">
                      {t('summary.startedAtLabel')}
                    </p>
                    <p className="cmp-summary-stat-value mt-1 text-sm">
                      {completedRunStartedAt || t('summary.notAvailable')}
                    </p>
                  </div>
                  <div className="cmp-summary-stat-card rounded-2xl border border-teal-100 bg-white/80 p-3 shadow-sm">
                    <p className="cmp-summary-stat-label text-xs font-semibold uppercase tracking-wide">
                      {t('summary.finishedAtLabel')}
                    </p>
                    <p className="cmp-summary-stat-value mt-1 text-sm">
                      {completedRunFinishedAt || t('summary.notAvailable')}
                    </p>
                  </div>
                  <div className="cmp-summary-stat-card rounded-2xl border border-teal-100 bg-white/80 p-3 shadow-sm">
                    <p className="cmp-summary-stat-label text-xs font-semibold uppercase tracking-wide">
                      {t('summary.durationLabel')}
                    </p>
                    <p className="cmp-summary-stat-value mt-1 text-sm">
                      {completedRunDuration || t('summary.notAvailable')}
                    </p>
                  </div>
                </div>

                <div className="cmp-summary-files mb-4 rounded-2xl border border-slate-200/70 bg-white/75 p-4 shadow-sm">
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <p className="cmp-summary-files-title text-sm font-semibold">{t('summary.filesTitle')}</p>
                    <span className="cmp-summary-files-count inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs">
                      <Files className="h-3.5 w-3.5 text-teal-600" />
                      {summaryFiles.length}
                    </span>
                  </div>
                  <div className="grid gap-2">
                    {summaryFiles.map((file) => {
                      const isOutput = file.type === 'output';
                      const roleLabel =
                        isOutput
                          ? t('summary.fileRoleOutput')
                          : file.type === 'pauta'
                            ? t('summary.fileRolePauta')
                            : t('summary.fileRolePdf');
                      const roleTone =
                        isOutput
                          ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                          : file.type === 'pauta'
                            ? 'border-sky-200 bg-sky-50 text-sky-700'
                            : 'border-amber-200 bg-amber-50 text-amber-700';
                      return (
                        <div
                          key={file.id}
                          className={
                            isOutput
                              ? 'cmp-summary-file-row cmp-summary-file-row--output flex items-center gap-3 rounded-xl border-2 border-emerald-200 bg-emerald-50/45 p-3.5 shadow-[0_10px_24px_rgba(16,185,129,0.12)] transition-colors duration-200 hover:border-emerald-300 hover:bg-emerald-50/65'
                              : 'cmp-summary-file-row flex items-center gap-3 rounded-xl border border-slate-200 bg-white p-3 transition-colors duration-200 hover:border-teal-200 hover:bg-teal-50/30'
                          }
                        >
                          <span
                            className={
                              isOutput
                                ? 'cmp-summary-file-icon cmp-summary-file-icon--output inline-flex h-10 w-10 items-center justify-center rounded-lg border border-emerald-200 bg-emerald-100 text-emerald-800'
                                : 'cmp-summary-file-icon inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-slate-50 text-slate-700'
                            }
                          >
                            {file.type === 'pdf' ? (
                              <FileText className="h-4 w-4" />
                            ) : (
                              <FileSpreadsheet className="h-4 w-4" />
                            )}
                          </span>
                          <span className="min-w-0 flex-1">
                            <span className="cmp-summary-file-name block truncate text-sm font-semibold">{file.name}</span>
                            <span className="mt-1 inline-flex items-center gap-2">
                              <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${roleTone}`}>
                                {roleLabel}
                              </span>
                              <span className="cmp-summary-file-ext text-[11px] font-medium">{fileExtension(file.name)}</span>
                            </span>
                          </span>
                          <a
                            href={file.downloadHref}
                            className={
                              isOutput
                                ? 'cmp-summary-download-primary inline-flex items-center gap-1 rounded-lg border border-emerald-600 bg-emerald-600 px-3 py-2 text-xs font-semibold text-white hover:bg-emerald-700'
                                : 'cmp-summary-download-secondary inline-flex items-center gap-1 rounded-lg border border-teal-200 bg-teal-50 px-3 py-2 text-xs font-semibold text-teal-700 hover:bg-teal-100'
                            }
                          >
                            <Download className="h-3.5 w-3.5" />
                            {t('summary.downloadFile')}
                          </a>
                        </div>
                      );
                    })}
                  </div>
                </div>

                <div className="mt-2 flex justify-center">
                  <Button
                    view="action"
                    size="l"
                    className="min-w-[220px] shadow-[0_10px_24px_rgba(13,148,136,0.28)]"
                    onClick={goToNewComparisonProject}
                  >
                    {t('newComparativa')}
                  </Button>
                </div>

                {!readOnly ? (
                <div className="cmp-summary-rerun-panel mt-5 rounded-2xl border border-slate-200/70 bg-white/80 p-4 shadow-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="cmp-summary-files-title mb-1 text-sm font-semibold">{t('summary.rerunTitle')}</p>
                      <p className="cmp-summary-file-ext text-xs">{t('summary.rerunHint')}</p>
                    </div>
                    <Button
                      view="outlined"
                      size="m"
                      disabled={isProcessing || rerunOverrideInProgress || !completedRun?.runId}
                      onClick={() => setShowRerunConfigurator((value) => !value)}
                    >
                      {showRerunConfigurator ? t('summary.rerunBackCta') : t('summary.rerunConfigureCta')}
                    </Button>
                  </div>

                  {showRerunConfigurator ? (
                    <div className="mt-4 space-y-3">
                      <div className="cmp-summary-rerun-block rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                        <p className="cmp-summary-file-ext mb-1 text-xs font-semibold uppercase tracking-wide">
                          {t('summary.fixedPautaTitle')}
                        </p>
                        <p className="cmp-summary-file-name truncate text-xs font-medium">
                          {completedRun?.pautaFilename || t('summary.notAvailable')}
                        </p>
                      </div>

                      <div className="cmp-summary-rerun-block rounded-xl border border-slate-200 bg-slate-50/60 p-3">
                        <p className="cmp-summary-file-ext mb-2 text-xs font-semibold uppercase tracking-wide">
                          {t('summary.rerunSelectionTitle')}
                        </p>
                        {existingRunPdfNames.length === 0 ? (
                          <p className="cmp-summary-file-ext text-xs">{t('summary.rerunNoOriginalPdfs')}</p>
                        ) : (
                          <div className="space-y-2">
                            {existingRunPdfNames.map((name) => {
                              const rerunAudit = rerunAuditSelection[name] !== false;
                              return (
                                <div key={name} className="cmp-summary-rerun-item flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2">
                                  <span className="cmp-summary-file-name truncate text-xs font-medium">{name}</span>
                                  <label className="cmp-summary-file-ext ml-3 flex items-center gap-2 text-xs">
                                    <input
                                      type="checkbox"
                                      checked={rerunAudit}
                                      onChange={(event) =>
                                        setRerunAuditSelection((previous) => ({
                                          ...previous,
                                          [name]: event.target.checked,
                                        }))
                                      }
                                    />
                                    {rerunAudit ? t('summary.rerunModeAudit') : t('summary.rerunModeReuse')}
                                  </label>
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </div>

                      <PdfUploader
                        files={rerunOverrideFiles}
                        onChange={setRerunOverrideFiles}
                        disabled={isProcessing || rerunOverrideInProgress}
                      />

                      <Button
                        view="outlined-action"
                        size="l"
                        width="max"
                        disabled={isProcessing || rerunOverrideInProgress || !completedRun?.runId}
                        onClick={handleRerunWithOverrides}
                      >
                        {t('summary.rerunCta')}
                      </Button>
                    </div>
                  ) : null}
                </div>
                ) : null}
              </SectionCard>
            </motion.div>
          </motion.div>
        )}

        {appState === 'processing' && (
          <motion.div key="processing">
            <ProcessingOverlay progress={progress} message={statusMsg} />
          </motion.div>
        )}

        {(appState === 'idle' || appState === 'error') && (
          <motion.div key="idle" className="presup-steps">
            <AnimatePresence>
              {appState === 'error' && (
                <motion.div
                  initial={{opacity: 0, height: 0}}
                  animate={{opacity: 1, height: 'auto'}}
                  exit={{opacity: 0, height: 0}}
                  className="presup-error-banner"
                >
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="12" cy="12" r="10" />
                    <line x1="12" y1="8" x2="12" y2="12" />
                    <line x1="12" y1="16" x2="12.01" y2="16" />
                  </svg>
                  <span>{errorMessage}</span>
                </motion.div>
              )}
            </AnimatePresence>

            {hasFailedRunRecovery ? (
              <motion.div custom={-1} variants={sectionVariants} initial="hidden" animate="visible">
                <SectionCard>
                  <StepBadge number={0} label={t('recovery.title')} />
                  <Text as="p" variant="body-1" color="secondary" style={{marginBottom: 10}}>
                    {t('recovery.subtitle')}
                  </Text>
                  <div className="presup-readiness-list" style={{marginBottom: 14}}>
                    <ReadinessRow
                      label={t('recovery.pauta', {name: lastFailedPautaFilename || '-'})}
                      ready
                    />
                    <ReadinessRow
                      label={t('recovery.pdfs', {count: lastFailedPdfFilenames.length})}
                      ready
                    />
                    {lastFailedPdfFilenames.map((name) => (
                      <ReadinessRow key={name} label={name} ready />
                    ))}
                  </div>
                  <Button
                    view="outlined-action"
                    size="l"
                    width="max"
                    disabled={isProcessing || rerunInProgress}
                    onClick={handleRerunLastFailed}
                  >
                    {t('recovery.cta')}
                  </Button>
                </SectionCard>
              </motion.div>
            ) : null}

            {/* Step 1 */}
            <motion.div custom={0} variants={sectionVariants} initial="hidden" animate="visible">
              <SectionCard>
                <StepBadge number={1} label={t('step1Label')} />
                <Text as="p" variant="body-1" color="secondary" style={{marginBottom: 16}}>
                  {t('step1Desc')}
                </Text>
                <ExcelUploader file={pautaFile} onChange={setPautaFile} disabled={isProcessing} />
              </SectionCard>
            </motion.div>

            <div className="presup-connector">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="var(--g-color-text-secondary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="6 9 12 15 18 9" />
              </svg>
            </div>

            {/* Step 2 */}
            <motion.div custom={1} variants={sectionVariants} initial="hidden" animate="visible">
              <SectionCard>
                <StepBadge number={2} label={t('step2Label')} />
                <Text as="p" variant="body-1" color="secondary" style={{marginBottom: 16}}>
                  {t('step2Desc')}
                </Text>
                <PdfUploader files={ofertaFiles} onChange={setOfertaFiles} disabled={isProcessing} />
              </SectionCard>
            </motion.div>

            <div className="presup-connector">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="var(--g-color-text-secondary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="6 9 12 15 18 9" />
              </svg>
            </div>

            {/* Step 3 */}
            <motion.div custom={2} variants={sectionVariants} initial="hidden" animate="visible">
              <SectionCard>
                <StepBadge number={3} label={t('step3Label')} />
                <Text as="p" variant="body-1" color="secondary" style={{marginBottom: 20}}>
                  {t('step3Desc')}
                </Text>

                <div className="presup-readiness-list">
                  <ReadinessRow
                    label={t('readiness.projectConnected')}
                    ready={hasPersistenceContext}
                  />
                  <ReadinessRow label={t('readiness.excel')} ready={pautaFile !== null} />
                  <ReadinessRow label={offersLabel} ready={ofertaFiles.length > 0} />
                </div>

                {shouldShowCreditEstimate ? (
                  <div className={`presup-credit-estimate ${creditEstimate.finalCredits !== null ? 'is-ready' : ''}`}>
                    <p className="presup-credit-estimate-title">{t('creditEstimate.title')}</p>
                    <p className="presup-credit-estimate-main">
                      {creditEstimate.loading
                        ? t('creditEstimate.loading')
                        : creditEstimate.finalCredits !== null
                          ? t('creditEstimate.main', {credits: creditEstimate.finalCredits})
                          : t('creditEstimate.pending')}
                    </p>
                    <p className="presup-credit-estimate-detail">
                      {t('creditEstimate.detail', {
                        files: fileSizesBytes.length,
                        mb: creditEstimate.totalMegabytes.toFixed(1),
                        base: creditEstimate.baseCredits,
                        pdf: creditEstimate.pdfCredits,
                        size: creditEstimate.sizeCredits,
                        margin: creditEstimate.marginPercent,
                      })}
                    </p>
                    {creditEstimate.sizeMode === 'per_file' ? (
                      <p className="presup-credit-estimate-detail presup-credit-estimate-detail--ok">
                        {t('creditEstimate.proportional')}
                      </p>
                    ) : null}
                  </div>
                ) : null}

                <Button
                  view="action"
                  size="xl"
                  width="max"
                  disabled={!canProcess || isProcessing}
                  onClick={handleProcess}
                >
                  {t('generate')}
                </Button>
              </SectionCard>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
