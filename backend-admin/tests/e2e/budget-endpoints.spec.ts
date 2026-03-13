import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

type MultipartFile = {
  name: string;
  mimeType: string;
  buffer: Buffer;
};

function makeFile(name: string, mimeType: string, content: string): MultipartFile {
  return {
    name,
    mimeType,
    buffer: Buffer.from(content, 'utf-8'),
  };
}

async function createAcceptedJob(request: { post: Function }): Promise<string> {
  const response = await request.post('/api/process-budget', {
    multipart: {
      pauta: makeFile(
        'pauta.xlsx',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'fake excel payload'
      ),
      files: makeFile('offer.pdf', 'application/pdf', '%PDF-1.4\n%%EOF'),
    },
  });

  expect(response.ok()).toBeTruthy();
  const body = await response.json();
  expect(typeof body.job_id).toBe('string');
  return body.job_id as string;
}

test('process-budget rejects invalid pauta extension', async ({ request }) => {
  const response = await request.post('/api/process-budget', {
    multipart: {
      pauta: makeFile('pauta.txt', 'text/plain', 'not excel'),
      files: makeFile('offer.pdf', 'application/pdf', '%PDF-1.4\n%%EOF'),
    },
  });

  expect(response.status()).toBe(422);
  const body = await response.json();
  expect(body.detail).toBe("'pauta' must be an Excel file (.xlsx or .xls).");
});

test('process-budget rejects non-PDF offer files', async ({ request }) => {
  const response = await request.post('/api/process-budget', {
    multipart: {
      pauta: makeFile(
        'pauta.xlsx',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'fake excel payload'
      ),
      files: makeFile('offer.txt', 'text/plain', 'not pdf'),
    },
  });

  expect(response.status()).toBe(422);
  const body = await response.json();
  expect(body.detail).toContain('not a PDF');
});

test('process-budget returns a job id for accepted uploads', async ({ request }) => {
  const jobId = await createAcceptedJob(request);
  expect(jobId).toMatch(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  );
});

test('status returns 404 for unknown job id', async ({ request }) => {
  const response = await request.get('/api/status/not-found-job-id');
  expect(response.status()).toBe(404);
  const body = await response.json();
  expect(body.detail).toBe('Job not found.');
});

test('status returns current state for created jobs', async ({ request }) => {
  const jobId = await createAcceptedJob(request);
  const response = await request.get(`/api/status/${jobId}`);

  expect(response.ok()).toBeTruthy();
  const body = await response.json();
  expect(body.job_id).toBe(jobId);
  expect(['processing', 'failed', 'completed']).toContain(body.status);
  expect(typeof body.progress).toBe('number');
  expect(typeof body.message).toBe('string');
});

test('download returns 404 for unknown job id', async ({ request }) => {
  const response = await request.get('/api/download/not-found-job-id');
  expect(response.status()).toBe(404);
  const body = await response.json();
  expect(body.detail).toBe('Job not found.');
});

test('download returns 409 when job is not completed', async ({ request }) => {
  const jobId = await createAcceptedJob(request);
  const response = await request.get(`/api/download/${jobId}`);

  expect(response.status()).toBe(409);
  const body = await response.json();
  expect(body.detail).toBe('Job is not completed yet.');
});

test('full flow with real files (optional)', async ({ request }) => {
  test.skip(
    process.env.E2E_RUN_FULL_FLOW !== '1',
    'Set E2E_RUN_FULL_FLOW=1 to run the end-to-end async pipeline test.'
  );

  const timeoutMs = Number(process.env.E2E_FULL_FLOW_TIMEOUT_MS ?? '900000');
  test.setTimeout(timeoutMs + 30_000);

  const fixturesDir = process.env.E2E_FIXTURES_DIR;
  const pautaPath = process.env.E2E_PAUTA_PATH ?? (fixturesDir ? path.join(fixturesDir, 'pauta.xlsx') : '');
  const pdfPath = process.env.E2E_PDF_PATH ?? (fixturesDir ? path.join(fixturesDir, 'offer.pdf') : '');

  test.skip(!pautaPath || !pdfPath, 'Provide E2E_PAUTA_PATH and E2E_PDF_PATH (or E2E_FIXTURES_DIR).');
  test.skip(!fs.existsSync(pautaPath), `Missing pauta file at ${pautaPath}.`);
  test.skip(!fs.existsSync(pdfPath), `Missing PDF file at ${pdfPath}.`);

  const startResponse = await request.post('/api/process-budget', {
    multipart: {
      pauta: {
        name: path.basename(pautaPath),
        mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        buffer: fs.readFileSync(pautaPath),
      },
      files: {
        name: path.basename(pdfPath),
        mimeType: 'application/pdf',
        buffer: fs.readFileSync(pdfPath),
      },
    },
  });

  expect(startResponse.ok()).toBeTruthy();
  const startBody = await startResponse.json();
  const jobId = String(startBody.job_id);

  const startedAt = Date.now();
  let finalPayload: Record<string, unknown> | null = null;

  while (Date.now() - startedAt < timeoutMs) {
    const statusResponse = await request.get(`/api/status/${jobId}`);
    expect(statusResponse.ok()).toBeTruthy();
    const payload = (await statusResponse.json()) as Record<string, unknown>;
    finalPayload = payload;

    if (payload.status === 'completed' || payload.status === 'failed') {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }

  expect(finalPayload).not.toBeNull();
  expect(finalPayload?.status).toBe('completed');

  const downloadResponse = await request.get(`/api/download/${jobId}`);
  expect(downloadResponse.ok()).toBeTruthy();
  const headers = downloadResponse.headers();
  expect(headers['content-type']).toContain('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  expect(headers['content-disposition']).toContain('comparativo_');
  const fileBody = await downloadResponse.body();
  expect(fileBody.byteLength).toBeGreaterThan(0);
});
