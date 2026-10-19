import L from 'leaflet';
import { useEffect, useRef } from 'react';
import 'leaflet/dist/leaflet.css';

import { formatNumber, formatPercent } from '../format';
import { t } from '../i18n';
import { binIndex } from '../model';
import { theme } from '../theme';
import { BinLegend } from './ChartLegend';
import type { DelayDirection, Language, StopMapPoint } from '../types';

interface StopMapProps {
  language: Language;
  mode: DelayDirection;
  points: StopMapPoint[];
  thresholds: number[];
  names: Map<string, string>;
}

function rampFor(mode: DelayDirection): readonly string[] {
  return mode === 'late' ? theme.lateRamp : theme.earlyRamp;
}

export default function StopMap({ language, mode, points, thresholds, names }: StopMapProps) {
  const hostRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);
  const copy = t(language);

  useEffect(() => {
    if (!hostRef.current || mapRef.current) return;
    const coarsePointer = window.matchMedia('(pointer: coarse)').matches;
    const map = L.map(hostRef.current, {
      center: [60.4518, 22.2666],
      zoom: 10,
      dragging: !coarsePointer,
      touchZoom: !coarsePointer,
      doubleClickZoom: !coarsePointer,
      boxZoom: !coarsePointer,
      scrollWheelZoom: false,
      preferCanvas: true,
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(map);
    const layer = L.layerGroup().addTo(map);
    mapRef.current = map;
    layerRef.current = layer;
    return () => {
      map.remove();
      mapRef.current = null;
      layerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const layer = layerRef.current;
    const map = mapRef.current;
    if (!layer || !map) return;
    layer.clearLayers();
    const ramp = rampFor(mode);
    const bounds: [number, number][] = [];
    // Draw low values first so the highest-value stops sit on top where markers overlap.
    const ordered = [...points].sort((a, b) => a.display_value - b.display_value);
    for (const point of ordered) {
      if (point.stop_lat === null || point.stop_lon === null) continue;
      const latLng: [number, number] = [point.stop_lat, point.stop_lon];
      bounds.push(latLng);
      const color = ramp[Math.min(binIndex(point.display_value, thresholds), ramp.length - 1)];
      const marker = L.circleMarker(latLng, {
        radius: Math.max(4, Math.min(11, 3 + Math.sqrt(point.bucket_count) / 12)),
        color: theme.surface,
        weight: 1.5,
        fillColor: color,
        fillOpacity: 0.92,
      });
      const popup = document.createElement('div');
      const heading = document.createElement('strong');
      heading.textContent = names.get(point.stop_id) ?? point.stop_name;
      const details = document.createElement('p');
      details.textContent = `${mode === 'late' ? copy.legendTitleLate : copy.legendTitleEarly}: ${formatPercent(point.display_value, language, 1)} · ${copy.onTime.toLowerCase()} ${formatPercent(point.pct_on_time, language, 1)} · ${formatNumber(point.bucket_count, language)} ${copy.buckets.toLowerCase()}`;
      popup.append(heading, details);
      marker.bindPopup(popup);
      marker.addTo(layer);
    }
    if (bounds.length && bounds.length < 80)
      map.fitBounds(bounds, { padding: [24, 24], maxZoom: 14 });
  }, [copy, language, mode, names, points, thresholds]);

  return (
    <div className="map-frame">
      <p className="map-hint">{copy.mapHint}</p>
      <BinLegend
        language={language}
        title={mode === 'late' ? copy.legendTitleLate : copy.legendTitleEarly}
        note={copy.legendNote}
        thresholds={thresholds}
        ramp={rampFor(mode)}
      />
      <section
        ref={hostRef}
        className="stop-map"
        aria-label={language === 'fi' ? 'Pysäkkien viivekartta' : 'Stop delay map'}
      />
    </div>
  );
}
