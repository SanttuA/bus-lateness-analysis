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
