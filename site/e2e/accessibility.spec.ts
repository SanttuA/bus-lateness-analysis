import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

for (const route of [
  '#/?view=table',
  '#/?view=table&line=24&direction=1&day=weekend&metric=early',
  '#/en?view=table',
  '#/en?view=table&line=24&direction=1&day=weekend&metric=early',
]) {
  test(`@a11y has no serious accessibility violations on ${route}`, async ({ page }) => {
    await page.goto(`./${route}`);
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('#stops .result-count')).toBeVisible();
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
      .analyze();
    const material = results.violations.filter(
      (violation) => violation.impact === 'serious' || violation.impact === 'critical',
    );
    expect(material).toEqual([]);
  });
}
