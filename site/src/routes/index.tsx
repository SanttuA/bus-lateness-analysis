import { createFileRoute } from '@tanstack/react-router';

import { ReportPage } from '../ReportPage';
import { validateSearch } from '../search';

export const Route = createFileRoute('/')({
  validateSearch,
  component: FinnishReport,
});

function FinnishReport() {
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <ReportPage
      language="fi"
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
