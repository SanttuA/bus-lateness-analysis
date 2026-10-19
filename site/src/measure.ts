/**
 * Text measurement for chart layout. Margins are sized from the page's own font, so labels fit
 * whichever system font the browser picks (the font stack has no web font).
 */

export const CHART_FONT_SIZE = 16;
/** Average glyph width in ems, used where canvas text metrics are unavailable (jsdom). */
const FALLBACK_EM = 0.6;

interface TextOptions {
  fontSize?: number;
  fontWeight?: number;
}

let canvasContext: CanvasRenderingContext2D | null | undefined;

function measuringContext(): CanvasRenderingContext2D | null {
  if (canvasContext !== undefined) return canvasContext;
  canvasContext = null;
  if (typeof document !== 'undefined' && !navigator.userAgent.includes('jsdom')) {
    canvasContext = document.createElement('canvas').getContext('2d');
  }
  return canvasContext;
}

function pageFontFamily() {
  return getComputedStyle(document.documentElement).fontFamily || 'sans-serif';
}

/** Width in pixels of the widest text (or line, for multi-line text). */
export function textWidth(texts: readonly string[], options: TextOptions = {}): number {
  const fontSize = options.fontSize ?? CHART_FONT_SIZE;
  const lines = texts.flatMap((text) => text.split('\n'));
  const context = measuringContext();
  if (!context) {
    return Math.ceil(Math.max(0, ...lines.map((line) => line.length * fontSize * FALLBACK_EM)));
  }
  context.font = `${options.fontWeight ?? 400} ${fontSize}px ${pageFontFamily()}`;
  return Math.ceil(Math.max(0, ...lines.map((line) => context.measureText(line).width)));
}

/** Greedy word wrap to `maxWidth` pixels; a single word longer than the line stays whole. */
export function wrapText(text: string, maxWidth: number, options: TextOptions = {}): string[] {
  const lines: string[] = [];
  let current = '';
  for (const word of text.split(' ')) {
    const candidate = current ? `${current} ${word}` : word;
    if (current && textWidth([candidate], options) > maxWidth) {
      lines.push(current);
      current = word;
    } else {
      current = candidate;
    }
  }
  if (current) lines.push(current);
  return lines;
}

/** Smallest step from `steps` whose spacing fits labels `labelWidth` wide with a gap. */
export function labelStep(
  spacing: number,
  labelWidth: number,
  steps: readonly number[] = [1, 2, 3, 4, 6, 8, 12],
  gap = 6,
): number {
  return steps.find((step) => step * spacing >= labelWidth + gap) ?? steps.at(-1) ?? 1;
}
