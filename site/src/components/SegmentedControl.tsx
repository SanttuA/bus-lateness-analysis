interface SegmentedControlProps {
  legend: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
}

export function SegmentedControl({ legend, value, options, onChange }: SegmentedControlProps) {
  return (
    <fieldset className="segmented-field">
      <legend>{legend}</legend>
      <div className="segmented">
        {options.map((option) => (
          <button
            key={option.value}
            type="button"
            aria-pressed={value === option.value}
            onClick={() => onChange(option.value)}
          >
            {option.label}
          </button>
        ))}
      </div>
    </fieldset>
  );
}
