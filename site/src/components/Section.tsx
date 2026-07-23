interface SectionProps {
  id?: string;
  number?: string;
  title: string;
  intro?: string;
  tone?: 'plain' | 'tinted' | 'dark';
  children: React.ReactNode;
}

export function Section({ id, number, title, intro, tone = 'plain', children }: SectionProps) {
  return (
    <section id={id} className={`report-section section-${tone}`}>
      <div className="shell">
        <div className="section-heading">
          {number ? <p className="section-number">{number}</p> : null}
          <div>
            <h2>{title}</h2>
            {intro ? <p className="section-intro">{intro}</p> : null}
          </div>
        </div>
        {children}
      </div>
    </section>
  );
}
