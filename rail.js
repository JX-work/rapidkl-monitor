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
const fs = require('fs');
const path = require('path');

const RAIL_GTFS_URL = 'https://api.data.gov.my/gtfs-static/prasarana?category=rapid-rail-kl';
const REFRESH_MS = 24 * 60 * 60_000;

// Optional data files (committed to repo). Loaded once at require time.
function loadJsonSafe(rel) {
  try {
    return JSON.parse(fs.readFileSync(path.join(__dirname, rel), 'utf8'));
  } catch (e) {
    console.warn(`[rail] optional data not loaded: ${rel} (${e.code || e.message})`);
    return null;
  }
}
const fares = loadJsonSafe('data/rail-fares.json');
const transfersData = loadJsonSafe('data/rail-transfers.json');

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

// ─── fare lookup ────────────────────────────────────────────────────────────
function getFare(fromStopId, toStopId) {
  if (!fares) return null;
  const map = fares.gtfs_stop_id_to_fare_code || {};
  const fromCode = map[fromStopId];
  const toCode = map[toStopId];
  if (!fromCode || !toCode) return null;
  const cashRow = (fares.cash || {})[fromCode];
  const cashlessRow = (fares.cashless || {})[fromCode];
  const cash = cashRow && typeof cashRow[toCode] === 'number' ? cashRow[toCode] : null;
  const cashless = cashlessRow && typeof cashlessRow[toCode] === 'number' ? cashlessRow[toCode] : null;
  let concession = null;
  if (cash != null) concession = Math.floor(cash * 0.5 / 0.10) * 0.10;
  return {
    cash, cashless,
    concession: concession != null ? Math.round(concession * 100) / 100 : null,
  };
}

// ─── build the transfer graph (once, from loaded data) ──────────────────────
// nodes = stop_ids; edges = adjacent stops on a line (cost = 1 hop) +
// transfer links between co-located stops (cost = 0 hops, but marks a change).
let graph = null;         // stop_id -> [{ to, kind:'ride'|'transfer', routeId }]
let stopToRoutes = null;  // stop_id -> Set(routeId)

function buildGraph() {
  graph = {};
  stopToRoutes = {};
  const addEdge = (a, b, kind, routeId) => {
    (graph[a] ||= []).push({ to: b, kind, routeId });
  };

  // ride edges: consecutive stops on each route (both directions)
  for (const [routeId, stopIds] of Object.entries(rail.routeStops)) {
    for (const sid of stopIds) {
      (stopToRoutes[sid] ||= new Set()).add(routeId);
    }
    for (let i = 0; i < stopIds.length - 1; i++) {
      addEdge(stopIds[i], stopIds[i+1], 'ride', routeId);
      addEdge(stopIds[i+1], stopIds[i], 'ride', routeId);
    }
  }

  // transfer edges: between co-located stops (from rail-transfers.json)
  const transfers = (transfersData && transfersData.transfers) || [];
  for (const t of transfers) {
    const sts = t.stations || [];
    for (let i = 0; i < sts.length; i++) {
      for (let j = 0; j < sts.length; j++) {
        if (i !== j && graph[sts[i]] !== undefined || rail.stops[sts[i]]) {
          if (i !== j) addEdge(sts[i], sts[j], 'transfer', null);
        }
      }
    }
  }
}

// ─── journey planning: BFS minimising transfers then hops ───────────────────
function planJourney(fromStopId, toStopId) {
  if (!rail.loaded) return { error: 'rail data not loaded' };
  if (!rail.stops[fromStopId]) return { error: 'origin not found' };
  if (!rail.stops[toStopId]) return { error: 'destination not found' };
  if (fromStopId === toStopId) return { error: 'origin and destination are the same' };
  if (!graph) buildGraph();

  // BFS over (stop, currentRoute). We prefer fewer transfers, so we do a
  // 0-1 BFS: transfer edges cost 1 (a "change"), ride edges cost 0 within
  // the queue ordering but we still track hop count for tie-breaking.
  const start = { stop: fromStopId, route: null };
  const key = s => `${s.stop}|${s.route || ''}`;
  const visited = new Set();
  // priority: transfers asc, then hops asc — use a simple Dijkstra-ish loop
  const pq = [{ stop: fromStopId, route: null, transfers: 0, hops: 0, path: [] }];

  while (pq.length) {
    // pop lowest (transfers, hops)
    pq.sort((a, b) => a.transfers - b.transfers || a.hops - b.hops);
    const cur = pq.shift();
    const k = key(cur);
    if (visited.has(k)) continue;
    visited.add(k);

    if (cur.stop === toStopId) {
      return finishJourney(cur, fromStopId, toStopId);
    }

    for (const edge of (graph[cur.stop] || [])) {
      const next = {
        stop: edge.to,
        route: edge.kind === 'ride' ? edge.routeId : cur.route,
        transfers: cur.transfers + (edge.kind === 'ride' && cur.route && cur.route !== edge.routeId ? 1 : 0)
                              + (edge.kind === 'transfer' ? 1 : 0),
        hops: cur.hops + (edge.kind === 'ride' ? 1 : 0),
        path: [...cur.path, { ...edge, from: cur.stop }],
      };
      if (!visited.has(key(next))) pq.push(next);
    }
  }
  return { error: 'no route found' };
}

function finishJourney(end, fromStopId, toStopId) {
  // Collapse the edge path into human-readable legs (grouped by route).
  const legs = [];
  let cur = null;
  for (const e of end.path) {
    if (e.kind === 'transfer') {
      if (cur) { legs.push(cur); cur = null; }
      continue;
    }
    if (!cur || cur.routeId !== e.routeId) {
      if (cur) legs.push(cur);
      cur = { routeId: e.routeId, from: e.from, to: e.to, stops: 1 };
    } else {
      cur.to = e.to; cur.stops++;
    }
  }
  if (cur) legs.push(cur);

  const legDetails = legs.map(l => {
    const route = rail.routes[l.routeId] || {};
    return {
      routeId: l.routeId,
      lineName: route.long || l.routeId,
      lineShort: route.short || l.routeId,
      color: route.color || '#888',
      fromStop: l.from,
      fromName: rail.stops[l.from]?.name || l.from,
      toStop: l.to,
      toName: rail.stops[l.to]?.name || l.to,
      stops: l.stops,
    };
  });

  const fare = getFare(fromStopId, toStopId);
  const totalStops = legDetails.reduce((s, l) => s + l.stops, 0);
  // rough time: ~2.5 min per stop + 4 min per transfer
  const transfers = Math.max(0, legDetails.length - 1);
  const estMinutes = Math.round(totalStops * 2.5 + transfers * 4);

  return {
    from: { stop_id: fromStopId, name: rail.stops[fromStopId]?.name },
    to: { stop_id: toStopId, name: rail.stops[toStopId]?.name },
    legs: legDetails,
    transfers,
    totalStops,
    estMinutes,
    fare,
  };
}

// ─── search all stations (for journey planner autocomplete) ─────────────────
function allStations() {
  // Deduplicate by name (co-located transfer stops share a name), but keep
  // one representative stop_id + which lines serve it.
  if (!graph) buildGraph();
  const byName = {};
  for (const [sid, stop] of Object.entries(rail.stops)) {
    const routes = [...(stopToRoutes[sid] || [])];
    if (routes.length === 0) continue;  // skip stops not on any known route
    const name = stop.name;
    if (!byName[name]) {
      byName[name] = { stop_id: sid, name, lat: stop.lat, lon: stop.lon, lines: new Set(routes) };
    } else {
      routes.forEach(r => byName[name].lines.add(r));
    }
  }
  return Object.values(byName)
    .map(s => ({ stop_id: s.stop_id, name: s.name, lat: s.lat, lon: s.lon,
                 lines: [...s.lines] }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

module.exports = {
  rail,
  loadRailGtfs,
  REFRESH_MS,
  nextTrains,
  listLines,
  lineStations,
  activeServiceIds,
  planJourney,
  getFare,
  allStations,
};
