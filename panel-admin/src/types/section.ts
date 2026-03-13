export interface ConfidenceFlag {
  label: string;
  score: number | null;
}

export interface SectionDraft {
  id: string;
  title: string;
  content: string;
  confidence: number | null;
  flags: ConfidenceFlag[];
}
