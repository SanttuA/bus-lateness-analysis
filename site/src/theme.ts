/**
 * Chart colours, validated with the dataviz palette checks against the card surface
 * (#fffdf8):
 * - late/early series pair: categorical, all checks pass (CVD ΔE 20).
 * - diverging arms and sequential ramps: ordinal checks pass (monotone lightness, light end
 *   at least 2:1 on the surface).
 * - weekend gray is the de-emphasis partner of the late accent (emphasis form), always with a
 *   legend and direct labels.
 */
export const theme = {
  surface: '#fffdf8',
  ink: '#1b282a',
  inkSoft: '#566261',
  muted: '#737b79',
  grid: '#e4ddd0',
  baseline: '#a9a399',
  band: '#efe8dc',
  late: '#c86c24',
  early: '#2a6fa8',
  deemphasis: '#6b7574',
  neutral: '#d6d0c4',
  punctuality: {
    earlyStrong: '#2d6493',
    earlyMild: '#8db6d6',
    onTime: '#d6d0c4',
    lateMild: '#e8a46c',
    lateStrong: '#b3541e',
  },
  lateRamp: ['#e8a46c', '#d6844a', '#bd652c', '#9a4c1c', '#723611'],
  earlyRamp: ['#8db6d6', '#679ccb', '#4680b6', '#2d6493', '#1b466b'],
} as const;

export const plotStyle = {
  background: 'transparent',
  color: theme.ink,
  fontFamily: 'inherit',
  fontSize: '16px',
  overflow: 'visible',
} as const;
