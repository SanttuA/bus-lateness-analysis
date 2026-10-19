import { lazy, Suspense, useEffect, useState } from 'react';

import { loadContext, loadLines, loadOverview } from './data';
import { formatDate, formatDuration, formatPercent } from './format';
import { dataLabel, t } from './i18n';
import type { ContextPayload, Language, LinesPayload, OverviewPayload, SearchState } from './types';
import { AlertChart } from './components/AlertChart';
import { DailyTrendChart } from './components/DailyTrendChart';
import { HourlyChart } from './components/HourlyChart';
import { InPageLink } from './components/InPageLink';
import { LineExplorer } from './components/LineExplorer';
import { MetricStrip } from './components/MetricStrip';
import { PunctualityChart } from './components/PunctualityChart';
import { RushChart } from './components/RushChart';
import { Section } from './components/Section';
import { SiteHeader } from './components/SiteHeader';

const StopExplorer = lazy(() => import('./components/StopExplorer'));

interface ReportPageProps {
  language: Language;
  search: SearchState;
  onSearchChange: (patch: Partial<SearchState>) => void;
}

interface ReportData {
  overview: OverviewPayload;
  lines: LinesPayload;
  context: ContextPayload;
}

export function ReportPage({ language, search, onSearchChange }: ReportPageProps) {
  const copy = t(language);
  const [data, setData] = useState<ReportData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    document.documentElement.lang = language;
    document.title = copy.title;
    const description = document.querySelector<HTMLMetaElement>('meta[name="description"]');
    if (description) description.content = copy.lede;
    const canonicalUrl = `https://santtua.github.io/bus-lateness-analysis/#/${language === 'en' ? 'en' : ''}`;
    const metadata: Array<[string, string]> = [
      ['meta[property="og:title"]', copy.title],
      ['meta[property="og:description"]', copy.lede],
      ['meta[property="og:url"]', canonicalUrl],
      ['meta[property="og:locale"]', language === 'fi' ? 'fi_FI' : 'en_GB'],
      ['meta[name="twitter:title"]', copy.title],
      ['meta[name="twitter:description"]', copy.lede],
    ];
    for (const [selector, content] of metadata) {
      const element = document.querySelector<HTMLMetaElement>(selector);
      if (element) element.content = content;
    }
    const canonical = document.querySelector<HTMLLinkElement>('link[rel="canonical"]');
    if (canonical) canonical.href = canonicalUrl;
  }, [copy.lede, copy.title, language]);

  useEffect(() => {
    Promise.all([loadOverview(), loadLines(), loadContext()])
      .then(([overview, lines, context]) => setData({ overview, lines, context }))
      .catch((reason: unknown) =>
        setError(reason instanceof Error ? reason.message : String(reason)),
      );
  }, []);

  if (error) {
    return (
      <main className="load-screen" id="main-content">
        <p role="alert">{copy.loadError}</p>
        <code>{error}</code>
        <button type="button" onClick={() => window.location.reload()}>
          {copy.retry}
        </button>
      </main>
    );
  }
  if (!data) {
    return (
      <main className="load-screen" id="main-content" aria-busy="true">
        {copy.loading}
      </main>
    );
  }

  const { overview, lines, context } = data;
  const headlines = overview.headlines[language];
  const qualityRows = context.quality.filter((row) => row.quality_check !== 'analysis_rows');
  const largestGaps = [...context.collector_gaps]
    .sort((a, b) => b.missing_min - a.missing_min)
    .slice(0, 4);
  const firstP90TakeawayIndex = overview.takeaways.findIndex((takeaway) =>
    takeaway[language].includes(copy.p90),
  );
  return (
    <>
      <InPageLink className="skip-link" targetId="main-content" focusTarget>
        Siirry sisältöön / Skip to content
      </InPageLink>
      <SiteHeader language={language} search={search} />
      <main id="main-content" tabIndex={-1}>
        <section className="hero" id="top">
          <div className="hero-orbit orbit-one" aria-hidden="true" />
          <div className="hero-orbit orbit-two" aria-hidden="true" />
          <div className="shell hero-grid">
            <div className="hero-copy">
              <p className="eyebrow">{copy.eyebrow}</p>
              <h1>{copy.title}</h1>
              <p className="lede">{copy.lede}</p>
              <p className="independent-badge">{copy.independent}</p>
            </div>
            <aside className="snapshot-card" aria-label={copy.snapshot}>
              <span>{copy.snapshot}</span>
              <strong>
                {formatDate(overview.summary.start_date, language)}
                <span aria-hidden="true"> → </span>
                <span className="sr-only"> – </span>
                {formatDate(overview.summary.end_date, language)}
              </strong>
              <p>{copy.staticNote}</p>
            </aside>
          </div>
          <div className="shell executive-summary">
            <h2>{copy.executive}</h2>
            <ul>
              {overview.takeaways.map((takeaway, index) => {
                const text = takeaway[language];
                const termStart = text.indexOf(copy.p90);
                if (index !== firstP90TakeawayIndex || termStart === -1) {
                  return <li key={takeaway.id}>{text}</li>;
                }
                return (
                  <li key={takeaway.id}>
                    {text.slice(0, termStart)}
                    <InPageLink
                      className="p90-definition-link"
                      targetId="p90-explanation"
                      ariaLabel={copy.p90ExplanationLink}
                      focusTarget
                    >
                      {copy.p90}
                    </InPageLink>
                    {text.slice(termStart + copy.p90.length)}
                  </li>
                );
              })}
            </ul>
          </div>
          <div className="shell">
            <MetricStrip language={language} overview={overview} />
            <p className="definition-note">
              <span aria-hidden="true">i</span>
              {copy.definition}
            </p>
          </div>
        </section>

        <Section id="findings" number="01" title={headlines.ranking} intro={copy.rankingIntro}>
          <PunctualityChart
            language={language}
            lines={lines.lines}
            frequentTripsPerDay={overview.meta.frequent_trips_per_day}
            search={search}
            onSearchChange={onSearchChange}
          />
        </Section>

        <Section number="02" title={headlines.hourly} intro={copy.hourlyIntro} tone="tinted">
          <HourlyChart language={language} rows={overview.hourly_profile} />
        </Section>

        <Section id="lines" number="03" title={copy.explorerTitle} intro={copy.explorerIntro}>
          <LineExplorer
            language={language}
            data={lines}
            search={search}
            onSearchChange={onSearchChange}
          />
        </Section>

        <Section number="04" title={headlines.rush} intro={copy.rushIntro} tone="tinted">
          <RushChart language={language} rows={context.rush_impact} />
        </Section>

        <Section id="alerts" number="05" title={headlines.alerts} intro={copy.alertsIntro}>
          <AlertChart
            language={language}
            rows={context.alerts}
            inventory={context.alert_inventory}
            cancellations={context.cancellations}
          />
        </Section>

        <Section
          id="stops"
          number="06"
          title={copy.stopsTitle}
          intro={copy.stopsIntro}
          tone="tinted"
        >
          <Suspense fallback={<p className="empty-state">{copy.loading}</p>}>
            <StopExplorer language={language} search={search} onSearchChange={onSearchChange} />
          </Suspense>
        </Section>

        <Section id="daily" number="07" title={headlines.daily} intro={copy.dailyIntro}>
          <DailyTrendChart language={language} rows={context.daily} />
        </Section>

        <Section number="08" title={copy.qualityTitle} tone="dark">
          <div className="action-grid">
            <div>
              <ol className="recommendations">
                {headlines.recommendations.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ol>
            </div>
            <div className="questions-card">
              <h3>{copy.questionsTitle}</h3>
              <ul>
                {copy.questions.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          </div>
        </Section>

        <Section id="methods" number="09" title={copy.methodsTitle} intro={copy.methodsIntro}>
          <div className="methods-grid">
            <div>
              <h3>{copy.caveatsTitle}</h3>
              <ul className="caveat-list">
                {overview.caveats.map((item) => (
                  <li key={item.id}>{item[language]}</li>
                ))}
              </ul>
            </div>
            <div className="method-card">
              <h3>{copy.filterTitle}</h3>
              <dl className="quality-list">
                {qualityRows.map((row) => (
                  <div key={row.quality_check}>
                    <dt>{dataLabel(language, 'qualityCheck', row.quality_check)}</dt>
                    <dd>{formatPercent(row.pct_rows, language, 2)}</dd>
                  </div>
                ))}
              </dl>
            </div>
            <div className="method-card gaps-card">
              <h3>{copy.gapsTitle}</h3>
              <ul>
                {largestGaps.map((gap) => (
                  <li key={`${gap.source}-${gap.gap_start_utc}`}>
                    <strong>{dataLabel(language, 'collector', gap.source)}</strong>
                    <span>{formatDuration(gap.missing_min, language)}</span>
                  </li>
                ))}
              </ul>
            </div>
          </div>
          <aside className="p90-explanation" aria-labelledby="p90-explanation">
            <h3 id="p90-explanation" tabIndex={-1}>
              {copy.p90ExplanationTitle}
            </h3>
            <p>{copy.p90Explanation}</p>
            <p>
              <strong>{copy.example}</strong> {copy.p90Example}
            </p>
          </aside>
        </Section>
      </main>
      <footer className="site-footer">
        <div className="shell footer-grid">
          <p>{copy.source}</p>
          <a href="https://github.com/SanttuA/bus-lateness-analysis">{copy.code}</a>
          <p>
            {copy.cacheBuilt}:{' '}
            <time dateTime={overview.meta.generated_at_utc}>
              {overview.meta.generated_at_utc.slice(0, 10)}
            </time>
          </p>
        </div>
      </footer>
    </>
  );
}
