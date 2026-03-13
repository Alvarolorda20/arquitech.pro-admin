'use client';

import {useState} from 'react';

interface TeamFeedbackAlertProps {
  message: string;
  tone: 'success' | 'warning' | 'error';
  closeLabel: string;
}

export function TeamFeedbackAlert({message, tone, closeLabel}: TeamFeedbackAlertProps) {
  const [visible, setVisible] = useState(true);
  if (!visible) return null;
  const isError = tone === 'error';

  return (
    <div
      className={`team-page-feedback team-page-feedback--${tone}`}
      role={isError ? 'alert' : 'status'}
      aria-live={isError ? 'assertive' : 'polite'}
    >
      <span className="team-page-feedback-message">{message}</span>
      <button
        type="button"
        className="team-page-feedback-close"
        aria-label={closeLabel}
        title={closeLabel}
        onClick={() => setVisible(false)}
      >
        &times;
      </button>
    </div>
  );
}