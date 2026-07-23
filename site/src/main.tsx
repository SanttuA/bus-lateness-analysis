import {
  createHashHistory,
  createRouter,
  parseSearchWith,
  RouterProvider,
  stringifySearchWith,
} from '@tanstack/react-router';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { routeTree } from './routeTree.gen';
import './styles.css';

const router = createRouter({
  routeTree,
  history: createHashHistory(),
  scrollRestoration: true,
  defaultPreload: 'intent',
  parseSearch: parseSearchWith((value) => value),
  stringifySearch: stringifySearchWith((value) => String(value)),
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

const root = document.getElementById('root');
if (!root) throw new Error('Root element not found');

createRoot(root).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
