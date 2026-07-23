import { createFileRoute } from '@tanstack/react-router';

import { ReportPage } from '../ReportPage';
import { validateSearch } from '../search';

export const Route = createFileRoute('/en')({
  validateSearch,
  component: EnglishReport,
});

function EnglishReport() {
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <ReportPage
      language="en"
      search={search}
      onSearchChange={(patch) =>
        void navigate({
          search: (previous) => ({ ...previous, ...patch }),
          replace: true,
          resetScroll: false,
        })
      }
    />
  );
}
