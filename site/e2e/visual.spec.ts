import { expect, test } from '@playwright/test';

test('hero visual remains stable at desktop and mobile widths', async ({ page }, testInfo) => {
  test.skip(
    !['chromium-desktop', 'mobile-chromium'].includes(testInfo.project.name),
    'Visual baselines are intentionally limited to Chromium desktop and mobile.',
  );
  await page.goto('./#/?view=table');
  await expect(page.locator('h1')).toBeVisible();
  await expect(page.locator('.hero')).toHaveScreenshot(`hero-${testInfo.project.name}.png`, {
    animations: 'disabled',
  });
});

test('ranking evidence remains legible at desktop and mobile widths', async ({
  page,
}, testInfo) => {
  test.skip(
    !['chromium-desktop', 'mobile-chromium'].includes(testInfo.project.name),
    'Visual baselines are intentionally limited to Chromium desktop and mobile.',
  );
  await page.goto('./#/?view=table');
  await page.addStyleTag({
    content: '.site-header, .skip-link { visibility: hidden !important; }',
  });
  const findings = page.locator('#findings');
  await findings.scrollIntoViewIfNeeded();
  await expect(findings).toHaveScreenshot(`findings-${testInfo.project.name}.png`, {
    animations: 'disabled',
  });
});

test('disruption groups remain distinct and legible at desktop and mobile widths', async ({
  page,
}, testInfo) => {
  test.skip(
    !['chromium-desktop', 'mobile-chromium'].includes(testInfo.project.name),
    'Visual baselines are intentionally limited to Chromium desktop and mobile.',
  );
  await page.goto('./#/?view=table');
  await page.addStyleTag({
    content: '.site-header, .skip-link { visibility: hidden !important; }',
  });
  const alerts = page.locator('#alerts');
  await alerts.scrollIntoViewIfNeeded();
  await expect(alerts).toHaveScreenshot(`alerts-${testInfo.project.name}.png`, {
    animations: 'disabled',
  });
});
