import { useEffect, useId, useRef } from 'react';

interface PlotFigureProps {
  title: string;
  subtitle: string;
  className?: string;
  render: (width: number) => Element;
  legend?: React.ReactNode;
  footnote?: React.ReactNode;
  scrollHint?: string;
  children?: React.ReactNode;
}

export function PlotFigure({
  title,
  subtitle,
  className,
  render,
  legend,
  footnote,
  scrollHint = 'Taulukkoa voi vierittää sivusuunnassa. / Scroll sideways to see every column.',
  children,
}: PlotFigureProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const titleId = useId();

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const draw = () => {
      const width = Math.max(280, Math.floor(container.getBoundingClientRect().width));
      container.replaceChildren(render(width));
    };
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(container);
    // Label margins are measured with the page font; redraw once a late-loading font is in.
    let active = true;
    void document.fonts?.ready.then(() => {
      if (active) draw();
    });
    return () => {
      active = false;
      observer.disconnect();
    };
  }, [render]);

  return (
    <figure className={className ? `plot-figure ${className}` : 'plot-figure'}>
      <figcaption>
        <h3 id={titleId}>{title}</h3>
        <p>{subtitle}</p>
      </figcaption>
      {legend}
      <div ref={containerRef} className="plot-host" aria-hidden="true" />
      {footnote ? <p className="figure-footnote">{footnote}</p> : null}
      <details className="chart-fallback">
        <summary aria-describedby={titleId}>Datataulukko / Data table</summary>
        <div>
          <p className="table-scroll-hint">{scrollHint}</p>
          {children}
        </div>
      </details>
    </figure>
  );
}
