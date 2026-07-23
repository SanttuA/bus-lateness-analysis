import { expect, test } from '@playwright/test';

test('reader-facing text stays at or above 16px', async ({ page }, testInfo) => {
  test.skip(
    !['chromium-desktop', 'mobile-chromium'].includes(testInfo.project.name),
    'Computed typography is covered at the visual-regression desktop and mobile sizes.',
  );
  await page.route(/tile\.openstreetmap\.org/, (route) => route.abort());
  await page.goto('./#/en?view=map');
  await expect(page.locator('h1')).toBeVisible();
  await expect(page.locator('.plot-host svg').first()).toBeVisible();
  await expect(page.getByRole('region', { name: 'Stop delay map' })).toBeVisible();

  const undersized = await page.locator('body').evaluate((body) =>
    [...body.querySelectorAll<HTMLElement | SVGElement>('*')]
      .filter((element) => {
        if (element.closest('.sr-only')) return false;
        const style = getComputedStyle(element);
        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
          return false;
        }
        const bounds = element.getBoundingClientRect();
        if (bounds.width === 0 || bounds.height === 0) return false;
        const ownsText = [...element.childNodes].some(
          (node) => node.nodeType === Node.TEXT_NODE && node.textContent?.trim(),
        );
        return ownsText || element.matches('button, input, select, summary');
      })
      .map((element) => ({
        element: `${element.tagName.toLowerCase()}.${element.getAttribute('class') ?? ''}`,
        size: Number.parseFloat(getComputedStyle(element).fontSize),
        text: element.textContent?.trim().replaceAll(/\s+/g, ' ').slice(0, 80),
      }))
      .filter(({ size }) => size < 15.9),
  );

  expect(undersized).toEqual([]);
});

test('chapter marker is a readable editorial element', async ({ page }, testInfo) => {
  test.skip(
    !['chromium-desktop', 'mobile-chromium'].includes(testInfo.project.name),
    'Chapter-marker design is covered at the visual-regression desktop and mobile sizes.',
  );
  await page.goto('./#/?view=table');
  const marker = page.locator('#findings .section-number');
  await expect(marker).toBeVisible();
  const design = await marker.evaluate((element) => {
    const style = getComputedStyle(element);
    const bounds = element.getBoundingClientRect();
    return {
      fontSize: Number.parseFloat(style.fontSize),
      letterSpacing: Number.parseFloat(style.letterSpacing),
      width: bounds.width,
      height: bounds.height,
      borderRadius: Number.parseFloat(style.borderRadius),
    };
  });

  const mobile = testInfo.project.name === 'mobile-chromium';

  expect(design.fontSize).toBeGreaterThanOrEqual(mobile ? 19 : 24);
  expect(Math.abs(design.letterSpacing)).toBeLessThanOrEqual(1);
  expect(design.width).toBeGreaterThanOrEqual(mobile ? 40 : 56);
  expect(design.height).toBeGreaterThanOrEqual(mobile ? 40 : 56);
  expect(design.borderRadius).toBeGreaterThanOrEqual(mobile ? 20 : 28);
});

test('mobile chapter heading forms a compact lockup', async ({ page }, testInfo) => {
  test.skip(
    !['mobile-chromium', 'mobile-webkit'].includes(testInfo.project.name),
    'This composition is mobile-only.',
  );
  await page.goto('./#/?view=table');

  const composition = await page.locator('#findings').evaluate((section) => {
    const heading = section.querySelector<HTMLElement>('.section-heading');
    const marker = section.querySelector<HTMLElement>('.section-number');
    const title = section.querySelector<HTMLElement>('h2');
    const intro = section.querySelector<HTMLElement>('.section-intro');
    if (!heading || !marker || !title || !intro) throw new Error('Incomplete section heading');

    const markerBounds = marker.getBoundingClientRect();
    const titleBounds = title.getBoundingClientRect();
    const introBounds = intro.getBoundingClientRect();
    const titleStyle = getComputedStyle(title);

    return {
      sectionPaddingTop: Number.parseFloat(getComputedStyle(section).paddingTop),
      headingMarginBottom: Number.parseFloat(getComputedStyle(heading).marginBottom),
      markerTitleTopDifference: Math.abs(markerBounds.top - titleBounds.top),
      textIndent: Number.parseFloat(titleStyle.textIndent),
      markerWidth: markerBounds.width,
      titleFontSize: Number.parseFloat(titleStyle.fontSize),
      titleHeight: titleBounds.height,
      titleIntroGap: introBounds.top - titleBounds.bottom,
    };
  });

  expect(composition.sectionPaddingTop).toBeLessThanOrEqual(56);
  expect(composition.headingMarginBottom).toBeLessThanOrEqual(28);
  expect(composition.markerTitleTopDifference).toBeLessThanOrEqual(3);
  expect(composition.textIndent).toBeGreaterThanOrEqual(composition.markerWidth + 12);
  expect(composition.titleFontSize).toBeGreaterThanOrEqual(33);
  expect(composition.titleFontSize).toBeLessThanOrEqual(41);
  expect(composition.titleHeight).toBeLessThanOrEqual(230);
  expect(composition.titleIntroGap).toBeGreaterThanOrEqual(12);
  expect(composition.titleIntroGap).toBeLessThanOrEqual(20);
});
