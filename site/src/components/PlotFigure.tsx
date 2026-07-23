import { useEffect, useId, useRef } from 'react';

interface PlotFigureProps {
  title: string;
  subtitle: string;
  className?: string;
  render: (width: number) => Element;
  children?: React.ReactNode;
}

export function PlotFigure({ title, subtitle, className, render, children }: PlotFigureProps) {
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
    return () => observer.disconnect();
  }, [render]);

  return (
    <figure className={className ? `plot-figure ${className}` : 'plot-figure'}>
      <figcaption>
        <h3 id={titleId}>{title}</h3>
        <p>{subtitle}</p>
      </figcaption>
      <div ref={containerRef} className="plot-host" aria-hidden="true" />
      <details className="chart-fallback">
        <summary aria-describedby={titleId}>Datataulukko / Data table</summary>
        <div>{children}</div>
      </details>
    </figure>
  );
}
