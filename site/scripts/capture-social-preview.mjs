import { chromium } from '@playwright/test';
import { fileURLToPath } from 'node:url';

const outputPath = fileURLToPath(new URL('../public/social-preview.png', import.meta.url));
const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: 1200, height: 630 },
  deviceScaleFactor: 1,
});
await page.setContent(`
  <!doctype html>
  <html lang="fi">
    <head>
      <meta charset="utf-8">
      <style>
        * { box-sizing: border-box; }
        html, body { width: 1200px; height: 630px; margin: 0; overflow: hidden; }
        body {
          position: relative;
          padding: 58px 68px;
          background: #f7f2e8;
          color: #1b282a;
          font-family: Inter, Avenir Next, Segoe UI, Arial, sans-serif;
        }
        .orbit { position: absolute; width: 570px; height: 570px; top: -340px; right: -90px; border: 2px solid #e7cdb5; border-radius: 50%; }
        .orbit::after { position: absolute; width: 330px; height: 330px; top: 118px; left: 120px; border: 2px solid #cbdce7; border-radius: 50%; content: ''; }
        .brand { display: flex; align-items: center; gap: 12px; font-size: 15px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; }
        .bars { display: flex; align-items: end; gap: 4px; height: 28px; }
        .bars i { display: block; width: 6px; border-radius: 6px; background: #c86c24; }
        .bars i:nth-child(1) { height: 14px; } .bars i:nth-child(2) { height: 28px; } .bars i:nth-child(3) { height: 20px; background: #316f9e; }
        .eyebrow { margin: 76px 0 14px; color: #8f4516; font-size: 14px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; }
        h1 { max-width: 880px; margin: 0; font-family: Charter, Georgia, serif; font-size: 86px; font-weight: 500; letter-spacing: -.045em; line-height: .98; }
        .sub { max-width: 780px; margin: 24px 0 0; color: #566261; font-size: 23px; line-height: 1.4; }
        .metrics { position: absolute; right: 68px; bottom: 54px; left: 68px; display: grid; grid-template-columns: repeat(3, 1fr); border-top: 1px solid #cfc6b8; }
        .metric { padding: 19px 22px 0 0; }
        .metric strong { display: block; font-family: Charter, Georgia, serif; font-size: 29px; font-weight: 500; }
        .metric span { color: #566261; font-size: 12px; font-weight: 800; letter-spacing: .06em; text-transform: uppercase; }
      </style>
    </head>
    <body>
      <div class="orbit"></div>
      <div class="brand"><span class="bars"><i></i><i></i><i></i></span>Föli / data</div>
      <p class="eyebrow">Riippumaton analyysi · huhti–toukokuu 2026</p>
      <h1>Fölin bussit:<br>täsmällisyys datassa</h1>
      <p class="sub">Missä bussit myöhästyvät — ja missä etuajassa kulkeminen korostuu?</p>
      <div class="metrics">
        <div class="metric"><strong>3,75 milj.</strong><span>matka–pysäkki-luokkaa</span></div>
        <div class="metric"><strong>140</strong><span>linjaa</span></div>
        <div class="metric"><strong>23.4.–23.5.2026</strong><span>julkaistu tilannekuva</span></div>
      </div>
    </body>
  </html>
`);
await page.screenshot({ path: outputPath, type: 'png' });
await browser.close();
console.log(`Wrote ${outputPath}`);
