import { expect, test } from '@playwright/test';

test('health endpoint responds OK', async ({ request }) => {
  const response = await request.get('/health');
  expect(response.ok()).toBeTruthy();

  const body = await response.json();
  expect(body).toMatchObject({ status: 'ok' });
});
