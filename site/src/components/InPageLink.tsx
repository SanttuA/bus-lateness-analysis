import type { MouseEvent, ReactNode } from 'react';

interface InPageLinkProps {
  targetId: string;
  children: ReactNode;
  className?: string;
  ariaLabel?: string;
  focusTarget?: boolean;
}

function isModifiedClick(event: MouseEvent<HTMLAnchorElement>) {
  return event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;
}

export function InPageLink({
  targetId,
  children,
  className,
  ariaLabel,
  focusTarget = false,
}: InPageLinkProps) {
  const currentRoute = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  return (
    <a
      className={className}
      href={currentRoute}
      aria-label={ariaLabel}
      onClick={(event) => {
        if (isModifiedClick(event)) return;
        event.preventDefault();
        const target = document.getElementById(targetId);
        if (!target) return;
        target.scrollIntoView({ block: 'start' });
        if (focusTarget) target.focus({ preventScroll: true });
      }}
    >
      {children}
    </a>
  );
}
