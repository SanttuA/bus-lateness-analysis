import { createRootRoute, Link, Outlet } from '@tanstack/react-router';

export const Route = createRootRoute({
  component: Outlet,
  notFoundComponent: () => (
    <main className="load-screen">
      <h1>404</h1>
      <p>Sivua ei löytynyt. Page not found.</p>
      <Link to="/" search={{}}>
        Palaa raporttiin / Return to report
      </Link>
    </main>
  ),
});
