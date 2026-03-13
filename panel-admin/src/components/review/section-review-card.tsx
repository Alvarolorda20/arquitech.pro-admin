'use client';

import {useState} from 'react';
import {Button} from '@gravity-ui/uikit';

import {ConfidenceBadge} from '@/components/ui/confidence-badge';
import type {SectionDraft} from '@/types/section';

interface SectionReviewCardProps {
  section: SectionDraft;
}

export function SectionReviewCard({section}: SectionReviewCardProps) {
  const [draft, setDraft] = useState(section.content);

  return (
    <article className="surface-card grid">
      <div className="section-meta">
        <h2 className="section-title">{section.title}</h2>
        <ConfidenceBadge label="Overall confidence" score={section.confidence} />
      </div>

      <div className="confidence-row">
        {section.flags.map((flag) => (
          <ConfidenceBadge key={flag.label} label={flag.label} score={flag.score} />
        ))}
      </div>

      <textarea
        className="section-textarea"
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        aria-label={`${section.title} editor`}
      />

      <div className="section-actions">
        <Button>Regenerate section</Button>
        <Button view="action">Save draft</Button>
      </div>
    </article>
  );
}
