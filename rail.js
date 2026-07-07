// rail.js — KL rail (LRT/MRT/Monorail/BRT) module for rapidkl-monitor.
//
// Loads the rapid-rail-kl GTFS *static* feed and derives:
//   • line list (routes.txt)
//   • stations per line (stops.txt + stop_times.txt ordering)
//   • "next train in N minutes" using frequency-based scheduling
//     (frequencies.txt headways + stop_times.txt base timings)
//   • simple A→B journey planning along/across lines
//
// The upstream realtime rail feed is 404 (Prasarana hasn't published it),
// so everything here is schedule-based prediction, not live positions.
//
// All data is fetched at startup and refreshed daily — nothing is stored on disk.

const unzipper = require('unzipper');

const RAIL_GTFS_URL = 'https://api.data.gov.my/gtfs-static/prasarana?category=rapid-rail-kl';
const REFRESH_MS = 24 * 60 * 60_000;

// ─── CSV parser (same minimal one used elsewhere) ──────────────────────────
function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.length > 0);
  if (lines.length === 0) return [];
  const parseLine = (line) => {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i+1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else {
        if (c === ',') { out.push(cur); cur = ''; }
        else if (c === '"' && cur === '') inQ = true;
        else cur += c;
      }
    }
    out.push(cur);
    return out;
  };
  const headers = parseLine(lines[0]).map(h => h.trim());
  return lines.slice(1).map(line => {
    const cells = parseLine(line);
    const obj = {};
    for (let i = 0; i < headers.length; i++) obj[headers[i]] = (cells[i] || '').trim();
    return obj;
  });
}

function hhmmssToSec(s) {
  if (!s) return null;
  const parts = s.split(':').map(Number);
  if (parts.length !== 3 || parts.some(isNaN)) return null;
  return parts[0] * 3600 + parts[1] * 60 + parts[2];
}
function secToHHMM(sec) {
  sec = ((sec % 86400) + 86400) % 86400;
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
}

// ─── module state ───────────────────────────────────────────────────────────
const rail = {
  loaded: false,
  lastLoad: null,
  routes: {},        // route_id -> { route_id, short, long, color, type }
  stops: {},         // stop_id -> { stop_id, name, lat, lon }
  trips: [],         // [{ trip_id, route_id, service_id, direction_id, shape_id }]
  stopTimes: {},     // trip_id -> [{ stop_id, seq, arr, dep }]
  frequencies: {},   // trip_id -> [{ start, end, headway }]
  calendar: {},      // service_id -> { mon..sun booleans, start, end }
  shapes: {},        // shape_id -> [[lat,lon],...]
  routeStops: {},    // route_id -> ordered [stop_id] (representative direction)
};

// ─── which GTFS calendar service applies today ──────────────────────────────
function activeServiceIds(now = new Date()) {
  // Map JS day (0=Sun..6=Sat) to GTFS calendar columns.
  const day = now.getDay();
  const dayField = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][day];
  const ymd = now.getFullYear() * 10000 + (now.getMonth()+1) * 100 + now.getDate();
  const ids = [];
  for (const [sid, c] of Object.entries(rail.calendar)) {
    if (c[dayField] !== '1') continue;
    if (c.start && ymd < c.start) continue;
    if (c.end && ymd > c.end) continue;
    ids.push(sid);
  }
  return ids;
}

// ─── load + parse GTFS zip ──────────────────────────────────────────────────
async function loadRailGtfs() {
  console.log('[rail] downloading GTFS static…');
  const res = await fetch(RAIL_GTFS_URL, { headers: { 'User-Agent': 'rapidkl-monitor/2.0' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const dir = await unzipper.Open.buffer(buf);

  const want = new Set(['routes.txt','stops.txt','trips.txt','stop_times.txt',
                        'frequencies.txt','calendar.txt','shapes.txt']);
  const texts = {};
  for (const entry of dir.files) {
    const name = entry.path.split('/').pop();
    if (want.has(name)) texts[name] = (await entry.buffer()).toString('utf8');
  }

  // routes
  const routes = {};
  for (const r of parseCsv(texts['routes.txt'] || '')) {
    routes[r.route_id] = {
      route_id: r.route_id,
      short: r.route_short_name || r.route_id,
      long: r.route_long_name || '',
      color: r.route_color ? `#${r.route_color}` : '#888',
      type: (r.route_long_name || '').match(/LRT|MRT|Monorail|BRT/i)?.[0] || 'Rail',
    };
  }

  // stops
  const stops = {};
  for (const s of parseCsv(texts['stops.txt'] || '')) {
    const lat = parseFloat(s.stop_lat), lon = parseFloat(s.stop_lon);
    stops[s.stop_id] = {
      stop_id: s.stop_id,
      name: s.stop_name || s.stop_id,
      lat: isNaN(lat) ? null : lat,
      lon: isNaN(lon) ? null : lon,
    };
  }

  // trips
  const trips = [];
  for (const t of parseCsv(texts['trips.txt'] || '')) {
    trips.push({
      trip_id: t.trip_id,
      route_id: t.route_id,
      service_id: t.service_id,
      direction_id: t.direction_id || '0',
      shape_id: t.shape_id || null,
    });
  }

  // stop_times grouped by trip
  const stopTimes = {};
  for (const st of parseCsv(texts['stop_times.txt'] || '')) {
    (stopTimes[st.trip_id] ||= []).push({
      stop_id: st.stop_id,
      seq: parseInt(st.stop_sequence, 10) || 0,
      arr: hhmmssToSec(st.arrival_time),
      dep: hhmmssToSec(st.departure_time),
    });
  }
  for (const id of Object.keys(stopTimes)) stopTimes[id].sort((a,b) => a.seq - b.seq);

  // frequencies grouped by trip
  const frequencies = {};
  for (const f of parseCsv(texts['frequencies.txt'] || '')) {
    (frequencies[f.trip_id] ||= []).push({
      start: hhmmssToSec(f.start_time),
      end: hhmmssToSec(f.end_time),
      headway: parseInt(f.headway_secs, 10) || 600,
    });
  }

  // calendar
  const calendar = {};
  for (const c of parseCsv(texts['calendar.txt'] || '')) {
    calendar[c.service_id] = {
      monday: c.monday, tuesday: c.tuesday, wednesday: c.wednesday,
      thursday: c.thursday, friday: c.friday, saturday: c.saturday, sunday: c.sunday,
      start: c.start_date ? parseInt(c.start_date, 10) : null,
      end: c.end_date ? parseInt(c.end_date, 10) : null,
    };
  }

  // shapes
  const shapesRaw = {};
  for (const s of parseCsv(texts['shapes.txt'] || '')) {
    (shapesRaw[s.shape_id] ||= []).push({
      seq: parseInt(s.shape_pt_sequence, 10) || 0,
      lat: parseFloat(s.shape_pt_lat),
      lon: parseFloat(s.shape_pt_lon),
    });
  }
  const shapes = {};
  for (const id of Object.keys(shapesRaw)) {
    shapes[id] = shapesRaw[id].sort((a,b)=>a.seq-b.seq).map(p => [p.lat, p.lon]);
  }

  // representative ordered stop list per route (use a direction_id=0 trip)
  const routeStops = {};
  for (const t of trips) {
    if (routeStops[t.route_id]) continue;
    if (t.direction_id !== '0') continue;
    const sts = stopTimes[t.trip_id];
    if (sts && sts.length) routeStops[t.route_id] = sts.map(s => s.stop_id);
  }
  // fallback: any trip if no direction-0 found
  for (const t of trips) {
    if (routeStops[t.route_id]) continue;
    const sts = stopTimes[t.trip_id];
    if (sts && sts.length) routeStops[t.route_id] = sts.map(s => s.stop_id);
  }

  Object.assign(rail, {
    loaded: true, lastLoad: Date.now(),
    routes, stops, trips, stopTimes, frequencies, calendar, shapes, routeStops,
  });

  console.log(`[rail] loaded ${Object.keys(routes).length} lines, ` +
              `${Object.keys(stops).length} stops, ${trips.length} trips, ` +
              `${Object.keys(shapes).length} shapes`);
}

// ─── next-train computation for a stop on a line ────────────────────────────
// Returns upcoming departures for the given stop, per direction, for the next
// `horizonMin` minutes, based on frequency expansion.
function nextTrains(routeId, stopId, now = new Date(), horizonMin = 60) {
  const services = new Set(activeServiceIds(now));
  const nowSec = now.getHours()*3600 + now.getMinutes()*60 + now.getSeconds();
  const horizonSec = nowSec + horizonMin * 60;

  const result = { direction0: [], direction1: [] };
  const dirLabels = { 0: null, 1: null };

  // relevant trips: matching route + active service
  const trips = rail.trips.filter(t => t.route_id === routeId && services.has(t.service_id));

  for (const trip of trips) {
    const sts = rail.stopTimes[trip.trip_id];
    if (!sts) continue;
    const stopEntry = sts.find(s => s.stop_id === stopId);
    if (!stopEntry) continue;

    // offset of this stop from the trip's first departure
    const firstDep = sts[0].dep ?? sts[0].arr ?? 0;
    const stopDep = stopEntry.dep ?? stopEntry.arr ?? firstDep;
    const offset = stopDep - firstDep;

    // terminus name = last stop of this trip (headsign proxy)
    const lastStopId = sts[sts.length - 1].stop_id;
    const headsign = rail.stops[lastStopId]?.name || '';
    const dir = trip.direction_id === '1' ? 1 : 0;
    if (!dirLabels[dir]) dirLabels[dir] = headsign;

    const freqs = rail.frequencies[trip.trip_id] || [];
    for (const f of freqs) {
      if (f.start == null || f.end == null) continue;
      // trains depart origin at f.start, f.start+headway, ... < f.end
      for (let depOrigin = f.start; depOrigin < f.end; depOrigin += f.headway) {
        const depAtStop = depOrigin + offset;
        if (depAtStop < nowSec) continue;
        if (depAtStop > horizonSec) break;
        result[`direction${dir}`].push({
          routeId,
          lineName: rail.routes[routeId]?.long || routeId,
          headsign,
          scheduledDeparture: secToHHMM(depAtStop),
          departureSeconds: depAtStop,
          minutesAway: Math.max(0, Math.round((depAtStop - nowSec) / 60)),
          directionId: dir,
        });
      }
    }
  }

  result.direction0.sort((a,b) => a.departureSeconds - b.departureSeconds);
  result.direction1.sort((a,b) => a.departureSeconds - b.departureSeconds);
  result.dir0Label = dirLabels[0];
  result.dir1Label = dirLabels[1];
  return result;
}

// ─── line list for the UI ───────────────────────────────────────────────────
function listLines() {
  return Object.values(rail.routes).map(r => {
    const stopIds = rail.routeStops[r.route_id] || [];
    return {
      route_id: r.route_id,
      short: r.short,
      long: r.long,
      color: r.color,
      type: r.type,
      stationCount: stopIds.length,
    };
  }).sort((a,b) => a.long.localeCompare(b.long));
}

// ─── stations of a line (ordered) ───────────────────────────────────────────
function lineStations(routeId) {
  const stopIds = rail.routeStops[routeId] || [];
  const route = rail.routes[routeId] || null;
  const shapeId = (rail.trips.find(t => t.route_id === routeId && t.shape_id) || {}).shape_id;
  return {
    route,
    shape: shapeId ? rail.shapes[shapeId] || [] : [],
    stations: stopIds.map(id => rail.stops[id]).filter(Boolean),
  };
}

module.exports = {
  rail,
  loadRailGtfs,
  REFRESH_MS,
  nextTrains,
  listLines,
  lineStations,
  activeServiceIds,
};
