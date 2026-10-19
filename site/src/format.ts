import type { Language } from './types';

const locales: Record<Language, string> = { fi: 'fi-FI', en: 'en-GB' };

export function formatNumber(value: number, language: Language, maximumFractionDigits = 0) {
  return new Intl.NumberFormat(locales[language], { maximumFractionDigits }).format(value);
}

export function formatPercent(value: number, language: Language, digits = 1) {
  return `${formatNumber(value, language, digits)} %`;
}

export function formatMinutes(value: number, language: Language, digits = 1) {
  return `${formatNumber(value, language, digits)} min`;
}

export function formatDate(value: string, language: Language) {
  return new Intl.DateTimeFormat(locales[language], {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
  }).format(new Date(`${value}T12:00:00Z`));
}

export function formatDuration(minutes: number, language: Language) {
  if (minutes < 120) return `${formatNumber(minutes, language, 0)} min`;
  return `${formatNumber(minutes / 60, language, 1)} h`;
}

export function formatAlertPriority(value: number, unknownLabel: string) {
  return value < 0 ? unknownLabel : String(value);
}

/** " (95 %: 16,6–20,6)" style interval suffix, or an empty string when unavailable. */
export function formatInterval(
  low: number | null | undefined,
  high: number | null | undefined,
  language: Language,
  unit: '%' | 'min' | 'pp',
  digits = 1,
) {
  if (low === null || low === undefined || high === null || high === undefined) return '';
  const label = language === 'fi' ? '95 %' : '95%';
  const suffix =
    unit === '%' ? ' %' : unit === 'pp' ? (language === 'fi' ? ' %-yks.' : ' pp') : ' min';
  return ` (${label}: ${formatNumber(low, language, digits)}–${formatNumber(high, language, digits)}${suffix})`;
}

export function formatSigned(value: number, language: Language, digits = 1) {
  const formatted = formatNumber(Math.abs(value), language, digits);
  if (value > 0) return `+${formatted}`;
  if (value < 0) return `−${formatted}`;
  return formatted;
}
