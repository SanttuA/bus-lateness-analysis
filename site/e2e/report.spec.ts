import { expect, test } from '@playwright/test';
import type { Locator, Page } from '@playwright/test';

async function expectUpdateToPreserveScroll(
  page: Page,
  control: Locator,
  update: () => Promise<void>,
  expectedUrl: RegExp,
) {
  await control.scrollIntoViewIfNeeded();
  const before = await page.evaluate(() => window.scrollY);
  await update();
  await expect(page).toHaveURL(expectedUrl);
  await page.evaluate(
    () =>
      new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))),
  );
  const after = await page.evaluate(() => window.scrollY);
  expect(Math.abs(after - before)).toBeLessThanOrEqual(4);
}

test('Finnish report presents the answer before the evidence', async ({ page }) => {
  await page.goto('./#/?view=table');
  await expect(
    page.getByRole('heading', { level: 1, name: 'Fölin bussit: täsmällisyys datassa' }),
  ).toBeVisible();
  await expect(page.getByRole('heading', { level: 2, name: 'Tiivistelmä' })).toBeVisible();
  await expect(page.getByText('pysäkkiohituksista aikataulussa')).toBeVisible();
  await expect(
    page.getByRole('heading', { level: 2, name: /ovat useimmin myöhässä/ }),
  ).toBeVisible();
  await expect(page.getByText(/95 %:n luottamusväli/).first()).toBeVisible();
});

test('first p90 mention links to a focused plain-language explanation', async ({ page }) => {
  await page.goto('./#/en?view=table');
  const originalUrl = page.url();
  const link = page.getByRole('link', {
    name: 'p90 delay — jump to a plain-language explanation',
  });

  await expect(link).toHaveText('p90 delay');
  await link.click();

  const explanation = page.getByRole('heading', { name: 'What does p90 mean?' });
  await expect(explanation).toBeFocused();
  await expect(page.getByText(/about 90 of every 100 stop passes/)).toBeVisible();
  await expect(page).toHaveURL(originalUrl);
});

test('English hash route survives reload and updates document language', async ({ page }) => {
  await page.goto('./#/en?view=table');
  await expect(
    page.getByRole('heading', { level: 1, name: 'Föli buses: punctuality in data' }),
  ).toBeVisible();
  await expect(page.locator('html')).toHaveAttribute('lang', 'en');
  await expect(page.locator('meta[property="og:title"]')).toHaveAttribute(
    'content',
    'Föli buses: punctuality in data',
  );
  await page.reload();
  await expect(page.getByRole('heading', { level: 2, name: 'Executive Summary' })).toBeVisible();
});

test('favicon is served from the GitHub Pages base path', async ({ page }) => {
  await page.goto('./#/');
  const icon = page.locator('link[rel="icon"]');
  await expect(icon).toHaveAttribute('href', '/bus-lateness-analysis/favicon.svg');
  const href = await icon.evaluate((element: HTMLLinkElement) => element.href);
  const response = await page.request.get(href);
  expect(response.ok()).toBe(true);
  expect(response.headers()['content-type']).toContain('image/svg+xml');
});

test('header links jump on the first click without changing the Finnish route', async ({
  page,
}) => {
  await page.goto('./#/?view=table&line=24');
  await expect(page.locator('h1')).toBeVisible();
  const originalUrl = page.url();

  await page.getByRole('link', { name: 'Linjat', exact: true }).click();

  await expect(page.locator('#lines')).toBeInViewport();
  await expect(page).toHaveURL(originalUrl);
  await expect(page.locator('html')).toHaveAttribute('lang', 'fi');
});

test('header links preserve English after the language switch', async ({ page }) => {
  await page.goto('./#/?view=table&line=24&day=weekend');
  await expect(page.locator('h1')).toBeVisible();
  await page.getByRole('link', { name: 'In English' }).click();
  await expect(page.locator('html')).toHaveAttribute('lang', 'en');
  await expect(page).toHaveURL(/#\/en\?/);
  const englishUrl = page.url();

  const expectEnglishJump = async (linkName: string, targetId: string) => {
    await page.getByRole('link', { name: linkName, exact: true }).click();
    await expect(page.locator(`#${targetId}`)).toBeInViewport();
    await expect(page).toHaveURL(englishUrl);
    await expect(page.locator('html')).toHaveAttribute('lang', 'en');
  };
  await expectEnglishJump('Findings', 'findings');
  await expectEnglishJump('Lines', 'lines');
  await expectEnglishJump('Stops', 'stops');
  await expectEnglishJump('Method', 'methods');
});

test('line explorer filters are reflected in the shareable URL', async ({ page }) => {
  await page.goto('./#/en?view=table');
  const explorer = page.locator('#lines');
  await explorer.getByRole('combobox', { name: 'Line', exact: true }).selectOption('24');
  await explorer.getByRole('button', { name: 'Over 3 min early', exact: true }).click();
  await expect(page).toHaveURL(/line=24/);
  await expect(page).toHaveURL(/heat=early/);
  await expect(explorer.getByRole('heading', { name: 'Line 24' })).toBeVisible();
  await expect(explorer.getByText(/Direction 1/).first()).toBeVisible();
});

test('line explorer controls update the URL without moving the page', async ({ page }) => {
  await page.goto('./#/en?view=table');
  await page.addStyleTag({ content: 'html { scroll-behavior: auto !important; }' });
  const explorer = page.locator('#lines');
  const line = explorer.getByRole('combobox', { name: 'Line', exact: true });
  const early = explorer.getByRole('button', { name: 'Over 3 min early', exact: true });

  await expectUpdateToPreserveScroll(page, line, () => line.selectOption('24'), /line=24/);
  await expectUpdateToPreserveScroll(page, early, () => early.click(), /heat=early/);
});

test('punctuality ranking switches between frequent and all lines', async ({ page }) => {
  await page.goto('./#/en?view=table');
  await page.addStyleTag({ content: 'html { scroll-behavior: auto !important; }' });
  const findings = page.locator('#findings');
  const all = findings.getByRole('button', { name: 'All', exact: true });
  await expectUpdateToPreserveScroll(page, all, () => all.click(), /lines=all/);
  await expect(findings.getByText(/incomplete data/).first()).toBeAttached();
  await findings.getByRole('button', { name: 'Early', exact: true }).click();
  await expect(page).toHaveURL(/rank=early/);
});

test('stop explorer exposes an exact table alternative', async ({ page }) => {
  await page.goto('./#/en?view=table');
  const stops = page.locator('#stops');
  await expect(stops.getByText(/stops in selection/)).toBeVisible();
  await stops.getByLabel('Stop name or ID').fill('Kauppatori');
  await expect(stops.getByRole('table')).toBeVisible();
  await expect(stops.getByText('Kauppatori', { exact: false }).first()).toBeVisible();
  const tableRegion = stops.getByRole('region', { name: /Exact values.*top 50 values/ });
  await tableRegion.focus();
  await expect(tableRegion).toBeFocused();
});

test('stop explorer switches between the table and keyboard-safe map', async ({ page }) => {
  await page.route(/tile\.openstreetmap\.org/, (route) => route.abort());
  await page.goto('./#/en?view=table');
  const stops = page.locator('#stops');
  await expect(stops.getByText(/stops in selection/)).toBeVisible();
  await stops.getByRole('button', { name: 'Map', exact: true }).click();
  await expect(page).toHaveURL(/view=map/);
  await expect(stops.getByRole('region', { name: 'Stop delay map' })).toBeVisible();
  await stops.getByRole('button', { name: 'Table', exact: true }).click();
  await expect(page).toHaveURL(/view=table/);
  await expect(stops.getByRole('table')).toBeVisible();
});

test('stop explorer controls update the URL without moving the page', async ({ page }) => {
  await page.route(/tile\.openstreetmap\.org/, (route) => route.abort());
  await page.goto('./#/en?view=table');
  await page.addStyleTag({ content: 'html { scroll-behavior: auto !important; }' });
  const stops = page.locator('#stops');
  await expect(stops.getByText(/stops in selection/)).toBeVisible();
  const line = stops.getByRole('combobox', { name: 'Line', exact: true });
  const day = stops.getByRole('combobox', { name: 'Day type', exact: true });
  const early = stops.getByRole('button', { name: 'Early', exact: true });
  const search = stops.getByLabel('Stop name or ID');
  const map = stops.getByRole('button', { name: 'Map', exact: true });

  await expectUpdateToPreserveScroll(page, line, () => line.selectOption('24'), /line=24/);
  await expectUpdateToPreserveScroll(page, day, () => day.selectOption('weekend'), /day=weekend/);
  await expectUpdateToPreserveScroll(page, early, () => early.click(), /metric=early/);
  await expectUpdateToPreserveScroll(
    page,
    search,
    () => search.fill('Kauppatori'),
    /stop=Kauppatori/,
  );
  await expectUpdateToPreserveScroll(page, map, () => map.click(), /view=map/);
});

test('keyboard navigation reaches the main report', async ({ page }) => {
  await page.goto('./#/?view=table');
  await expect(page.locator('h1')).toBeVisible();
  await page.keyboard.press('Tab');
  await expect(page.getByRole('link', { name: /Siirry sisältöön/ })).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.locator('#main-content')).toBeFocused();
});

test('explorers use readable labels and a useful default profile', async ({ page }, testInfo) => {
  await page.goto('./#/?view=table');
  await expect(page.locator('#lines').getByRole('heading', { name: 'Linja 3' })).toBeVisible();

  const labels = page.locator('#stops .filter-grid label > span, #stops .filter-grid legend');
  const styles = await labels.evaluateAll((elements) =>
    elements.map((element) => ({
      transform: getComputedStyle(element).textTransform,
      weight: Number(getComputedStyle(element).fontWeight),
    })),
  );
  expect(styles.every(({ transform, weight }) => transform === 'none' && weight <= 700)).toBe(true);

  await page.locator('#alerts summary').click();
  const alertTable = page.locator('#alerts table');
  await expect(
    alertTable.getByRole('rowheader', {
      name: /^Muu syy · Poikkeusreitti · Linja · tiedoteprioriteetti 1000$/,
    }),
  ).toHaveCount(1);
  await expect(
    alertTable.getByRole('rowheader', {
      name: /Poikkeusreitti · Pysäkki · tiedoteprioriteetti 1000/,
    }),
  ).toHaveCount(1);
  await expect(page.getByText('Tuntematon vaikutus', { exact: false }).first()).toBeVisible();
  await expect(
    page.getByText(/Fölin antamissa prioriteettiluvuissa pienempi luku tarkoittaa tärkeämpää/),
  ).toBeVisible();
  await expect(page.getByText(/Jos prioriteetti puuttuu lähdedatasta/)).toBeVisible();
  await expect(page.getByText('other cause', { exact: true })).toHaveCount(0);
  if (testInfo.project.name.startsWith('mobile')) {
    await expect(page.getByRole('navigation', { name: 'Päävalikko' })).toBeVisible();
    await expect(
      page.locator('#stops').getByText('Taulukkoa voi vierittää sivusuunnassa.', { exact: true }),
    ).toBeVisible();
  }
});

test('missing alert priorities are presented as unknown', async ({ page }) => {
  await page.route(/\/data\/context\.json$/, async (route) => {
    const response = await route.fetch();
    const payload = (await response.json()) as { alerts: Array<{ priority: number }> };
    for (const alert of payload.alerts) alert.priority = -1;
    await route.fulfill({ response, json: payload });
  });
  await page.goto('./#/en?view=table');

  const alerts = page.locator('#alerts');
  // Narrow screens shorten the chart label to 'prio.'; the full wording stays in the table.
  await expect(alerts.getByText(/(message priority|prio\.) unknown/).first()).toBeVisible();
  await expect(alerts.getByText('message priority -1', { exact: false })).toHaveCount(0);
});

test('daily trend replaces the stop-change list and marks holidays', async ({ page }) => {
  await page.goto('./#/en?view=table');
  const daily = page.locator('#daily');
  await expect(daily.getByRole('heading', { name: 'Late and early running by day' })).toBeVisible();
  await daily.getByText('Datataulukko / Data table').click();
  await expect(
    daily.getByRole('row', { name: /Weekends and holidays \(holiday\)/ }).first(),
  ).toBeVisible();
  await expect(page.getByText(/Largest stop-level p90 changes/)).toHaveCount(0);
});

test('line explorer only advertises lines with hourly profiles', async ({ page }) => {
  await page.goto('./#/?view=table');

  const lineSelector = page.locator('#lines select').first();
  await expect(lineSelector).toBeVisible();
  const advertisedLines = await lineSelector
    .locator('option')
    .evaluateAll((options) => options.map((option) => (option as HTMLOptionElement).value));
  const contextLines = await page.evaluate(async () => {
    const response = await fetch(new URL('data/lines.json', document.baseURI));
    if (!response.ok) throw new Error(`Unable to load lines.json (${response.status})`);
    const payload = (await response.json()) as { contexts: Array<{ line_ref: string }> };
    return [...new Set(payload.contexts.map((row) => row.line_ref))];
  });

  expect(advertisedLines.length).toBeGreaterThan(0);
  expect(advertisedLines.filter((line) => !contextLines.includes(line))).toEqual([]);
});
