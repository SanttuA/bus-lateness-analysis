import L from 'leaflet';
import { useEffect, useRef } from 'react';
import 'leaflet/dist/leaflet.css';

import { formatMinutes, formatNumber } from '../format';
import { t } from '../i18n';
import type { DelayDirection, Language, StopMapPoint } from '../types';

interface StopMapProps {
  language: Language;
  mode: DelayDirection;
  points: StopMapPoint[];
}

export default function StopMap({ language, mode, points }: StopMapProps) {
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
    const bounds: [number, number][] = [];
    for (const point of points) {
      if (point.stop_lat === null || point.stop_lon === null) continue;
      const latLng: [number, number] = [point.stop_lat, point.stop_lon];
      bounds.push(latLng);
      const marker = L.circleMarker(latLng, {
        radius: Math.max(4, Math.min(12, 3 + Math.sqrt(point.bucket_count) / 10)),
        color: mode === 'late' ? '#8f4516' : '#1f4f73',
        weight: 1,
        fillColor: mode === 'late' ? '#d77a31' : '#4f8fbd',
        fillOpacity: 0.68,
      });
      const popup = document.createElement('div');
      const heading = document.createElement('strong');
      heading.textContent = point.stop_name;
      const details = document.createElement('p');
      details.textContent = `${mode === 'late' ? copy.p90 : copy.earlyP90}: ${formatMinutes(point.display_value, language, 2)} · ${formatNumber(point.bucket_count, language)} ${copy.buckets.toLowerCase()}`;
      popup.append(heading, details);
      marker.bindPopup(popup);
      marker.addTo(layer);
    }
    if (bounds.length && bounds.length < 80)
      map.fitBounds(bounds, { padding: [24, 24], maxZoom: 14 });
  }, [copy.buckets, copy.earlyP90, copy.p90, language, mode, points]);

  return (
    <div className="map-frame">
      <p className="map-hint">{copy.mapHint}</p>
      <section
        ref={hostRef}
        className="stop-map"
        aria-label={language === 'fi' ? 'Pysäkkien viivekartta' : 'Stop delay map'}
      />
    </div>
  );
}
