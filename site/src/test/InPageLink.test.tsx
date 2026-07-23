import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { InPageLink } from '../components/InPageLink';

describe('InPageLink', () => {
  it('scrolls once without replacing the hash-router URL', async () => {
    const scrollIntoView = vi.fn<(arg?: boolean | ScrollIntoViewOptions) => void>();
    Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', {
      configurable: true,
      value: scrollIntoView,
    });
    window.history.replaceState({}, '', '/bus-lateness-analysis/#/en?view=table&line=24');
    const originalUrl = window.location.href;
    const user = userEvent.setup();
    render(
      <>
        <InPageLink targetId="lines">Lines</InPageLink>
        <section id="lines">Line explorer</section>
      </>,
    );

    await user.click(screen.getByRole('link', { name: 'Lines' }));

    expect(scrollIntoView).toHaveBeenCalledOnce();
    expect(window.location.href).toBe(originalUrl);
  });
});
