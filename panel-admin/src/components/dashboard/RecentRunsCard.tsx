'use client';

import {useEffect, useMemo, useState} from 'react';
import Link from 'next/link';
import {Button} from '@gravity-ui/uikit';
import {getPublicBackendApiBaseUrl} from '@/lib/backend-api';

interface RecentRunItem {
  id: string;
  status: string;
  pipelineJobId: string | null;
  projectName: string;
  statusLabel: string;
  statusTone: 'ok' | 'warn' | 'danger' | 'progress' | 'muted';
  dateLabel: string;
  progress: number | null;
  progressMessage: string | null;
  viewHref: string;
  downloadHref: string | null;
}

interface RecentRunsCardProps {
  title: string;
  description: string;
  runs: RecentRunItem[];
  projectCount: number;
  emptyRunsTitle: string;
  emptyRunsDescription: string;
  emptyProjectsTitle: string;
  emptyProjectsDescription: string;
  uploadCtaLabel: string;
  uploadCtaHref: string | null;
  createProjectCtaLabel: string;
  createProjectCtaHref: string | null;
  viewCtaLabel: string;
  downloadCtaLabel: string;
}

function toneClass(tone: RecentRunItem['statusTone']): string {
  if (tone === 'ok') return 'project-chip--ok';
  if (tone === 'warn') return 'project-chip--warn';
  if (tone === 'danger') return 'project-chip--danger';
  if (tone === 'progress') return 'project-chip--progress';
  return 'project-chip--muted';
}

export function RecentRunsCard({
  title,
  description,
  runs,
  projectCount,
  emptyRunsTitle,
  emptyRunsDescription,
  emptyProjectsTitle,
  emptyProjectsDescription,
  uploadCtaLabel,
  uploadCtaHref,
  createProjectCtaLabel,
  createProjectCtaHref,
  viewCtaLabel,
  downloadCtaLabel,
}: RecentRunsCardProps) {
  const hasProjects = projectCount > 0;
  const [liveRuns, setLiveRuns] = useState<RecentRunItem[]>(runs);
  const baseUrl = useMemo(() => getPublicBackendApiBaseUrl(), []);

  useEffect(() => {
    setLiveRuns(runs);
  }, [runs]);

  useEffect(() => {
    const activeRuns = liveRuns.filter(
      (run) =>
        Boolean(run.pipelineJobId) &&
        (run.status === 'running' || run.status === 'queued' || run.status === 'processing'),
    );
    if (activeRuns.length === 0) return;

    let cancelled = false;
    const poll = async () => {
      const updates = await Promise.all(
        activeRuns.map(async (run) => {
          try {
            const res = await fetch(
              `${baseUrl}/api/status/${encodeURIComponent(String(run.pipelineJobId || ''))}`,
              {cache: 'no-store'},
            );
            if (!res.ok) return null;
            const payload = (await res.json()) as {
              status?: string;
              progress?: number;
              message?: string;
            };
            const status = String(payload.status || '').trim().toLowerCase();
            const progress = typeof payload.progress === 'number' ? payload.progress : run.progress;
            const message = typeof payload.message === 'string' ? payload.message : run.progressMessage;
            return {
              runId: run.id,
              status,
              progress,
              message,
            };
          } catch {
            return null;
          }
        }),
      );

      if (cancelled) return;
      const valid = updates.filter((item) => Boolean(item));
      if (valid.length === 0) return;

      const finished = valid.some(
        (item) => item?.status === 'completed' || item?.status === 'failed' || item?.status === 'cancelled',
      );
      setLiveRuns((current) =>
        current.map((run) => {
          const found = valid.find((item) => item?.runId === run.id);
          if (!found) return run;
          return {
            ...run,
            status: found.status || run.status,
            progress: found.progress ?? run.progress,
            progressMessage: found.message ?? run.progressMessage,
          };
        }),
      );
      if (finished) {
        window.location.reload();
      }
    };

    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 4000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [baseUrl, liveRuns]);

  return (
    <section className="workspace-panel workspace-ops-panel workspace-runs-card">
      <div className="workspace-runs-head">
        <span className="workspace-runs-head-badge" aria-hidden="true">
          <span className="workspace-runs-head-badge-core" />
        </span>
        <div className="workspace-runs-head-copy">
          <h2 className="section-title">{title}</h2>
          <p className="workspace-panel-text">{description}</p>
        </div>
      </div>

      {!hasProjects ? (
        <div className="workspace-empty-state workspace-runs-empty">
          <div className="workspace-runs-empty-icon" aria-hidden="true">
            <span className="workspace-runs-empty-icon-core" />
          </div>
          <div className="workspace-runs-empty-copy">
            <p className="workspace-empty-title">{emptyProjectsTitle}</p>
            <p className="workspace-panel-text">{emptyProjectsDescription}</p>
          </div>
          {createProjectCtaHref ? (
            <div className="section-actions align-left">
              <Link href={createProjectCtaHref}>
                <Button view="action" size="m">
                  {createProjectCtaLabel}
                </Button>
              </Link>
            </div>
          ) : null}
        </div>
      ) : liveRuns.length === 0 ? (
        <div className="workspace-empty-state workspace-runs-empty">
          <div className="workspace-runs-empty-icon" aria-hidden="true">
            <span className="workspace-runs-empty-icon-core" />
          </div>
          <div className="workspace-runs-empty-copy">
            <p className="workspace-empty-title">{emptyRunsTitle}</p>
            <p className="workspace-panel-text">{emptyRunsDescription}</p>
          </div>
          {uploadCtaHref ? (
            <div className="section-actions align-left">
              <Link href={uploadCtaHref}>
                <Button view="action" size="m">
                  {uploadCtaLabel}
                </Button>
              </Link>
            </div>
          ) : null}
        </div>
      ) : (
        <ul className="workspace-runs-list">
          {liveRuns.map((run) => (
            <li key={run.id} className="workspace-runs-item">
              <span className={`workspace-runs-status-mark workspace-runs-status-mark--${run.statusTone}`} aria-hidden="true" />
              <div className="workspace-runs-meta">
                <p className="workspace-runs-project">{run.projectName}</p>
                <div className="workspace-runs-submeta">
                  <span className={`project-chip ${toneClass(run.statusTone)}`}>{run.statusLabel}</span>
                  <span className="workspace-panel-text">{run.dateLabel}</span>
                </div>
                {run.statusTone === 'progress' && run.progress !== null ? (
                  <div className="workspace-runs-progress-wrap">
                    <div className="workspace-runs-progress">
                      <span
                        className="workspace-runs-progress-bar"
                        style={{width: `${Math.min(100, Math.max(0, run.progress))}%`}}
                      />
                    </div>
                    <span className="workspace-runs-progress-value">
                      {Math.round(run.progress)}%
                    </span>
                    {run.progressMessage ? (
                      <span className="workspace-panel-text workspace-runs-progress-message">
                        {run.progressMessage}
                      </span>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="workspace-runs-actions">
                <Link href={run.viewHref}>
                  <Button view="outlined" size="m">
                    {viewCtaLabel}
                  </Button>
                </Link>
                {run.downloadHref ? (
                  <Link href={run.downloadHref}>
                    <Button view="outlined-action" size="m">
                      {downloadCtaLabel}
                    </Button>
                  </Link>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
