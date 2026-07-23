import { Link } from '@tanstack/react-router';

import { t } from '../i18n';
import type { Language, SearchState } from '../types';
import { InPageLink } from './InPageLink';

export function SiteHeader({ language, search }: { language: Language; search: SearchState }) {
  const copy = t(language);
  const otherPath = language === 'fi' ? '/en' : '/';
  return (
    <header className="site-header">
      <div className="shell header-inner">
        <InPageLink className="wordmark" targetId="top" ariaLabel={copy.title}>
          <span className="wordmark-mark" aria-hidden="true">
            <span />
            <span />
            <span />
          </span>
          <span>Föli / data</span>
        </InPageLink>
        <nav aria-label={language === 'fi' ? 'Päävalikko' : 'Primary navigation'}>
          <InPageLink targetId="findings">{copy.navFindings}</InPageLink>
          <InPageLink targetId="lines">{copy.navLines}</InPageLink>
          <InPageLink targetId="stops">{copy.navStops}</InPageLink>
          <InPageLink targetId="methods">{copy.navMethods}</InPageLink>
        </nav>
        <Link className="language-link" to={otherPath} search={search} resetScroll={false}>
          <span aria-hidden="true">{copy.languageCode}</span>
          <span className="sr-only">{copy.language}</span>
        </Link>
      </div>
    </header>
  );
}
