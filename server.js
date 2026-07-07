// Rapid KL Operations Monitor — backend
//
// Two data sources:
//   1. data.gov.my GTFS-Realtime (vehicle-position) — buses only. Prasarana
//      has not yet published stable rail realtime feeds.
//   2. myrapid.com.my RSS feed — official press releases & service notices,
//      used as the source of truth for LRT/MRT/bus disruptions.
//
// Pushes a combined snapshot to browsers over WebSocket. Static frontend
// served from ./public, with /alerts.html being the announcements view.

const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const GtfsRealtimeBindings = require('gtfs-realtime-bindings');
const unzipper = require('unzipper');
const { Readable } = require('stream');
const railModule = require('./rail');

const PORT = process.env.PORT || 3000;

// ─── polling intervals ─────────────────────────────────────────────────────
const BUS_POLL_MS   = 60_000;          // 60s for GTFS vehicle positions
const RSS_POLL_MS   = 5 * 60_000;      // 5min for the WordPress RSS feed
const BACKOFF_MAX_MS = 10 * 60_000;

// ─── service disruption heuristics (for buses) ─────────────────────────────
const FLEET_DROP_RATIO = 0.4;
const FLEET_LOW_RATIO  = 0.7;
const BASELINE_WINDOW  = 20;

// ─── GTFS-RT feed catalogue ────────────────────────────────────────────────
// KL bus + MRT feeder bus. Removed rapid-rail-kl (upstream unstable).
const FEEDS = [
  { id: 'bus-kl',        url: 'https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl' },
  { id: 'bus-mrtfeeder', url: 'https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-mrtfeeder' },
];

// ─── bus groups ────────────────────────────────────────────────────────────
const GROUPS = [
  { id: 'BUS-KL', name: 'Rapid Bus KL',   short: 'BUS', color: '#1f6feb', feed: 'bus-kl' },
  { id: 'BUS-MF', name: 'MRT Feeder Bus', short: 'MF',  color: '#7c5cff', feed: 'bus-mrtfeeder' },
];

// ─── GTFS Static (routes + shapes) ─────────────────────────────────────────
// Downloaded once at startup, refreshed daily. We merge data from both the KL
// bus and MRT feeder datasets so the route picker covers all live vehicles.
const GTFS_STATIC_SOURCES = [
  { id: 'rapid-bus-kl',        url: 'https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-kl' },
  { id: 'rapid-bus-mrtfeeder', url: 'https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-mrtfeeder' },
];
const GTFS_REFRESH_MS = 24 * 60 * 60_000;   // 24 hours

// ─── announcement classification ───────────────────────────────────────────
// Maps RSS items to a coarse category so the UI can color-code them.
// Match is case-insensitive against title + (first 200 chars of) description.
const CATEGORIES = [
  { id: 'rail-disruption', label: 'Rail Disruption', tone: 'error',
    test: t => /(gangguan|disruption|insiden|incident|baik pulih|breakdown|kerosakan).*(rail|laluan|kelana|ampang|kajang|putrajaya|monorail|mrt|lrt)/i.test(t)
            || /(rail|laluan|kelana|ampang|kajang|putrajaya|monorail|mrt|lrt).*(gangguan|disruption|insiden|incident|baik pulih|breakdown|kerosakan)/i.test(t) },
  { id: 'bus-disruption',  label: 'Bus Disruption',  tone: 'warn',
    test: t => /(bus|bas).*(gangguan|disruption|tidak|cancel|suspend|delay|lewat)/i.test(t)
            || /(gangguan|disruption|suspend|cancel).*bus|bas/i.test(t) },
  { id: 'route-change',    label: 'Route Change',    tone: 'info',
    test: t => /(route|laluan|perubahan|change|kemas kini|update|pengalihan|diversion|detour)/i.test(t) },
  { id: 'special-ops',     label: 'Special Operations', tone: 'info',
    test: t => /(special|khas|extended|tambahan|extra|extension|operasi)/i.test(t) },
  { id: 'promotion',       label: 'Promotion',       tone: 'mute',
    test: t => /(promosi|promotion|pas|pass|diskaun|discount|percuma|free|harga|price)/i.test(t) },
  { id: 'media',           label: 'Media Release',   tone: 'mute',
    test: t => /(kenyataan media|media release|press)/i.test(t) },
];

function classifyAnnouncement(title, desc) {
  const t = `${title} ${desc || ''}`.slice(0, 400);
  for (const c of CATEGORIES) if (c.test(t)) return c;
  return { id: 'general', label: 'General', tone: 'mute' };
}

// ───────────────────────────────────────────────────────────────────────────
// state
// ───────────────────────────────────────────────────────────────────────────
const state = {
  lastBusUpdate: null,
  lastRssUpdate: null,
  groups: Object.fromEntries(GROUPS.map(g => [g.id, blankGroup(g)])),
  busAlerts: [],
  announcements: [],
  feedHealth: Object.fromEntries(FEEDS.map(f => [f.id, { ok: false, lastErr: null, backoffUntil: 0 }])),
  rssHealth: { ok: false, lastErr: null },
  history: Object.fromEntries(GROUPS.map(g => [g.id, []])),
  seenAnnouncementUrls: new Set(),
  // GTFS Static (loaded once at startup, refreshed daily)
  routes: {},        // route_id -> { route_id, route_short_name, route_long_name, route_color }
  shapes: {},        // shape_id -> [[lat, lon], ...]
  tripToShape: {},   // trip_id -> shape_id
  routeToShapes: {}, // route_id -> Set(shape_id)
  gtfsStaticLoaded: false,
  gtfsStaticLastLoad: null,
};

function blankGroup(g) {
  return { ...g, status: 'unknown', vehicleCount: 0, vehicles: [],
           baseline: 0, lastSeen: null };
}

function pushBusAlert(level, groupId, message) {
  const now = Date.now();
  if (state.busAlerts.find(a => a.groupId === groupId && a.level === level &&
      a.message === message && now - a.ts < 10 * 60_000)) return;
  state.busAlerts.unshift({ id: `${now}-${Math.random().toString(36).slice(2,7)}`,
                            ts: now, level, groupId, message });
  state.busAlerts = state.busAlerts.slice(0, 50);
}

// ───────────────────────────────────────────────────────────────────────────
// GTFS-RT polling
// ───────────────────────────────────────────────────────────────────────────
async function fetchFeed(feed) {
  const health = state.feedHealth[feed.id];
  if (Date.now() < health.backoffUntil)
    throw new Error(`backoff until ${new Date(health.backoffUntil).toISOString()}`);
  const res = await fetch(feed.url, {
    headers: { 'User-Agent': 'rapidkl-monitor/2.0 (open-source dashboard)' },
  });
  if (res.status === 429) {
    const retryAfter = Number(res.headers.get('retry-after')) || 120;
    health.backoffUntil = Date.now() + Math.min(retryAfter * 1000, BACKOFF_MAX_MS);
    throw new Error(`HTTP 429 (backing off ${retryAfter}s)`);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  return GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf);
}

async function pollBuses() {
  const now = Date.now();
  const buckets = Object.fromEntries(GROUPS.map(g => [g.id, []]));

  for (const feed of FEEDS) {
    try {
      const msg = await fetchFeed(feed);
      state.feedHealth[feed.id].ok = true;
      state.feedHealth[feed.id].lastErr = null;
      const group = GROUPS.find(g => g.feed === feed.id);
      if (!group) continue;
      for (const ent of msg.entity) {
        const v = ent.vehicle;
        if (!v || !v.position) continue;
        buckets[group.id].push({
          id: (v.vehicle && v.vehicle.id) || ent.id,
          lat: v.position.latitude,
          lon: v.position.longitude,
          bearing: v.position.bearing || null,
          speed: v.position.speed || null,
          routeId: v.trip && v.trip.routeId,
          ts: v.timestamp ? Number(v.timestamp) * 1000 : now,
        });
      }
    } catch (e) {
      state.feedHealth[feed.id].ok = false;
      state.feedHealth[feed.id].lastErr = e.message;
      console.error(`[${feed.id}] ${e.message}`);
    }
  }

  for (const g of GROUPS) {
    const vs = buckets[g.id];
    const prev = state.groups[g.id];

    const hist = state.history[g.id];
    hist.push(vs.length);
    while (hist.length > BASELINE_WINDOW) hist.shift();
    const nonZero = hist.filter(n => n > 0);
    const baseline = nonZero.length ? Math.max(...nonZero) : 0;

    let status;
    if (!state.feedHealth[g.feed].ok)                       status = 'no-data';
    else if (baseline === 0 && vs.length === 0)             status = 'no-data';
    else if (baseline > 0 && vs.length === 0)               status = 'disrupted';
    else if (baseline >= 3 && vs.length/baseline < FLEET_DROP_RATIO) status = 'disrupted';
    else if (baseline >= 3 && vs.length/baseline < FLEET_LOW_RATIO)  status = 'delayed';
    else                                                    status = 'normal';

    if (prev.status !== 'unknown' && prev.status !== status) {
      if (status === 'disrupted')
        pushBusAlert('error', g.id, `${g.name}: fleet dropped to ${vs.length}/${baseline}`);
      else if (status === 'delayed')
        pushBusAlert('warn',  g.id, `${g.name}: reduced service (${vs.length}/${baseline})`);
      else if (status === 'normal' && (prev.status === 'delayed' || prev.status === 'disrupted'))
        pushBusAlert('info',  g.id, `${g.name}: service back to normal`);
    }

    state.groups[g.id] = { ...g, status, vehicleCount: vs.length,
      vehicles: vs, baseline,
      lastSeen: vs.length ? Math.max(...vs.map(v => v.ts)) : prev.lastSeen };
  }

  state.lastBusUpdate = now;
  broadcast({ type: 'snapshot', payload: snapshot() });
}

// ───────────────────────────────────────────────────────────────────────────
// RSS polling — myrapid.com.my announcements
// ───────────────────────────────────────────────────────────────────────────
function decodeEntities(s) {
  if (!s) return s;
  return s
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&').replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'").replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}
function stripTags(s) { return decodeEntities(s).replace(/<[^>]*>/g, '').trim(); }

function pickTag(block, tag) {
  // matches <tag>...</tag> or <tag attr="x">...</tag>, including CDATA
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i');
  const m = block.match(re);
  return m ? m[1] : '';
}

function parseRss(xml) {
  const items = [];
  const itemRe = /<item[^>]*>([\s\S]*?)<\/item>/gi;
  let m;
  while ((m = itemRe.exec(xml)) !== null) {
    const block = m[1];
    const title = stripTags(pickTag(block, 'title'));
    const link  = stripTags(pickTag(block, 'link'));
    const pub   = stripTags(pickTag(block, 'pubDate'));
    const desc  = stripTags(pickTag(block, 'description'));
    const cats  = [];
    const catRe = /<category[^>]*>([\s\S]*?)<\/category>/gi;
    let cm; while ((cm = catRe.exec(block)) !== null) cats.push(stripTags(cm[1]));
    if (!title || !link) continue;
    items.push({ title, link, pubDate: pub, description: desc, categories: cats,
                 ts: pub ? Date.parse(pub) : Date.now() });
  }
  return items;
}

// Fetch the myrapid RSS feed. Some hosts (e.g. datacenter IPs like Railway)
// are blocked by myrapid's Incapsula WAF, so we fall back to public read
// proxies that fetch from a different IP range.
async function fetchRssXml() {
  const target = 'https://myrapid.com.my/feed/';
  const attempts = [
    // 1. direct — works from residential IPs / local dev
    { label: 'direct', url: target,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; rapidkl-monitor/2.0)',
                 'Accept': 'application/rss+xml, application/xml, text/xml' } },
    // 2. r.jina.ai — must ask for raw text, else it returns Markdown
    { label: 'jina', url: `https://r.jina.ai/${target}`,
      headers: { 'User-Agent': 'Mozilla/5.0',
                 'X-Return-Format': 'text',
                 'X-Respond-With': 'text',
                 'Accept': 'text/xml, application/xml, */*' } },
    // 3. corsproxy.io — simple raw passthrough
    { label: 'corsproxy', url: `https://corsproxy.io/?url=${encodeURIComponent(target)}`,
      headers: { 'User-Agent': 'Mozilla/5.0' } },
    // 4. allorigins raw — last resort (can be slow / 408)
    { label: 'allorigins', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(target)}`,
      headers: { 'User-Agent': 'Mozilla/5.0' } },
  ];

  let lastErr = null;
  for (const a of attempts) {
    try {
      const res = await fetch(a.url, { headers: a.headers, signal: AbortSignal.timeout(25000) });
      if (!res.ok) { lastErr = `${a.label} HTTP ${res.status}`; continue; }
      const text = await res.text();
      // Accept if it looks like the feed: an <item>, an <rss, or the title we
      // know is in myrapid's feed. jina in text mode may strip angle brackets,
      // so also accept if it clearly contains feed-like article structure.
      const looksLikeFeed = /<item[\s>]/i.test(text) || /<rss[\s>]/i.test(text)
                            || /<channel[\s>]/i.test(text);
      if (looksLikeFeed) {
        if (a.label !== 'direct') console.log(`[rss] fetched via ${a.label} proxy`);
        return text;
      }
      lastErr = `${a.label} returned non-feed content (${text.length} chars)`;
    } catch (e) {
      lastErr = `${a.label}: ${e.name === 'TimeoutError' ? 'timeout' : e.message}`;
    }
  }
  throw new Error(lastErr || 'all fetch attempts failed');
}

async function pollRss() {
  try {
    const xml = await fetchRssXml();
    const items = parseRss(xml);

    const enriched = items.map(it => {
      const cat = classifyAnnouncement(it.title, it.description);
      return { ...it, category: cat.id, categoryLabel: cat.label, tone: cat.tone };
    });

    // detect newly seen items for popup notification
    const newOnes = [];
    for (const it of enriched) {
      if (!state.seenAnnouncementUrls.has(it.link)) {
        state.seenAnnouncementUrls.add(it.link);
        newOnes.push(it);
      }
    }

    state.announcements = enriched;
    state.rssHealth = { ok: true, lastErr: null };
    state.lastRssUpdate = Date.now();
    console.log(`[rss] fetched ${enriched.length} announcements (${newOnes.length} new)`);

    // first run: don't push notifications for the entire backlog
    const isFirstRun = state.seenAnnouncementUrls.size === enriched.length;
    if (!isFirstRun) {
      broadcast({ type: 'new-announcements', payload: newOnes.filter(n =>
        n.tone === 'error' || n.tone === 'warn') });
    }
    broadcast({ type: 'snapshot', payload: snapshot() });
  } catch (e) {
    state.rssHealth = { ok: false, lastErr: e.message };
    console.error(`[rss] ${e.message}`);
  }
}

// ───────────────────────────────────────────────────────────────────────────
// GTFS Static — download zip once, parse routes.txt + trips.txt + shapes.txt
// ───────────────────────────────────────────────────────────────────────────
function parseCsv(text) {
  // Minimal CSV parser — handles quoted fields with commas inside.
  // GTFS files are well-formed; no need for a full RFC 4180 implementation.
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
  const headers = parseLine(lines[0]);
  return lines.slice(1).map(line => {
    const cells = parseLine(line);
    const obj = {};
    for (let i = 0; i < headers.length; i++) obj[headers[i]] = cells[i] || '';
    return obj;
  });
}

async function fetchAndParseGtfsZip(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'rapidkl-monitor/2.0' },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  console.log(`[gtfs-static] ${url.split('=').pop()}: ${buf.length} bytes`);

  const dir = await unzipper.Open.buffer(buf);
  const filesNeeded = new Set(['routes.txt', 'trips.txt', 'shapes.txt']);
  const texts = {};
  for (const entry of dir.files) {
    const name = entry.path.split('/').pop();
    if (filesNeeded.has(name)) {
      texts[name] = (await entry.buffer()).toString('utf8');
    }
  }
  if (!texts['routes.txt'])  throw new Error('routes.txt missing in zip');
  if (!texts['shapes.txt'])  throw new Error('shapes.txt missing in zip');
  if (!texts['trips.txt'])   throw new Error('trips.txt missing in zip');
  return texts;
}

async function loadGtfsStatic() {
  console.log('[gtfs-static] downloading…');

  // Fetch every static source in parallel; if one fails, keep what we have.
  const results = await Promise.allSettled(
    GTFS_STATIC_SOURCES.map(src => fetchAndParseGtfsZip(src.url)
      .then(texts => ({ src, texts })))
  );

  const routes = {};
  const shapesRaw = {};
  const tripToShape = {};
  const routeToShapes = {};

  for (const r of results) {
    if (r.status !== 'fulfilled') {
      console.error('[gtfs-static]', r.reason.message);
      continue;
    }
    const { src, texts } = r.value;

    for (const row of parseCsv(texts['routes.txt'])) {
      routes[row.route_id] = {
        route_id: row.route_id,
        route_short_name: row.route_short_name || '',
        route_long_name: row.route_long_name || '',
        route_color: row.route_color ? `#${row.route_color}` : null,
        source: src.id,
      };
    }

    for (const s of parseCsv(texts['shapes.txt'])) {
      const id = s.shape_id;
      if (!shapesRaw[id]) shapesRaw[id] = [];
      shapesRaw[id].push({
        seq: parseInt(s.shape_pt_sequence, 10) || 0,
        lat: parseFloat(s.shape_pt_lat),
        lon: parseFloat(s.shape_pt_lon),
      });
    }

    for (const t of parseCsv(texts['trips.txt'])) {
      if (t.shape_id) {
        tripToShape[t.trip_id] = t.shape_id;
        if (!routeToShapes[t.route_id]) routeToShapes[t.route_id] = new Set();
        routeToShapes[t.route_id].add(t.shape_id);
      }
    }
  }

  const shapes = {};
  for (const id of Object.keys(shapesRaw)) {
    shapes[id] = shapesRaw[id]
      .sort((a, b) => a.seq - b.seq)
      .map(p => [p.lat, p.lon]);
  }

  state.routes = routes;
  state.shapes = shapes;
  state.tripToShape = tripToShape;
  state.routeToShapes = routeToShapes;
  state.publicIdToInternal = {};   // reset so /api/routes rebuilds it lazily
  state.gtfsStaticLoaded = Object.keys(routes).length > 0;
  state.gtfsStaticLastLoad = Date.now();
  console.log(`[gtfs-static] loaded ${Object.keys(routes).length} routes, ${Object.keys(shapes).length} shapes`);
}

// ───────────────────────────────────────────────────────────────────────────
function snapshot() {
  return {
    lastBusUpdate: state.lastBusUpdate,
    lastRssUpdate: state.lastRssUpdate,
    busPollMs: BUS_POLL_MS,
    rssPollMs: RSS_POLL_MS,
    groups: Object.values(state.groups),
    busAlerts: state.busAlerts.slice(0, 20),
    announcements: state.announcements,
    feedHealth: state.feedHealth,
    rssHealth: state.rssHealth,
    gtfsStaticLoaded: state.gtfsStaticLoaded,
  };
}

// ───────────────────────────────────────────────────────────────────────────
const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/api/snapshot', (_req, res) => res.json(snapshot()));
app.get('/healthz', (_req, res) => res.json({ ok: true,
  lastBus: state.lastBusUpdate, lastRss: state.lastRssUpdate }));

// List of all known routes (sorted by display name) for the route picker.
//
// Two ID quirks we hide from the frontend:
//   • rapid-bus-kl uses route_id like "U5810" which matches the realtime
//     feed verbatim.
//   • rapid-bus-mrtfeeder uses opaque ids like "30000125" with the public
//     name (e.g. "T807") in route_long_name. The realtime feed uses "T807".
//
// We expose a single `route_id` that the frontend should also pass when
// requesting shapes — internally we map it back to the GTFS route_id.
function publicRouteId(r) {
  // If long_name looks like a public route code (letters+digits, ≤ 6 chars),
  // prefer it — it's what the realtime feed will use.
  const ln = (r.route_long_name || '').trim();
  if (/^[A-Z]{0,3}\d{2,4}[A-Z]?$/i.test(ln)) return ln.toUpperCase();
  // Otherwise fall back to short_name then to internal id.
  const sn = (r.route_short_name || '').trim();
  if (sn) return sn;
  return r.route_id;
}

// Lookup: public_id -> internal route_id (so we can find shapes when the
// frontend asks for /api/route/T807/shapes).
function rebuildPublicIdIndex() {
  const idx = {};
  for (const r of Object.values(state.routes)) {
    const pid = publicRouteId(r);
    // first writer wins; if two routes share the same public id we keep the
    // first one (rare; would point to a data quality issue upstream)
    if (!idx[pid]) idx[pid] = r.route_id;
  }
  state.publicIdToInternal = idx;
}

app.get('/api/routes', (_req, res) => {
  // Make sure the index is up to date (it's cheap; ~250 entries)
  if (!state.publicIdToInternal || Object.keys(state.publicIdToInternal).length === 0) {
    rebuildPublicIdIndex();
  }
  const seen = new Set();
  const list = Object.values(state.routes)
    .filter(r => state.routeToShapes[r.route_id])
    .map(r => {
      const pid = publicRouteId(r);
      return {
        route_id: pid,                          // public id (matches realtime)
        internal_id: r.route_id,                // GTFS internal id (used by /api/route/:id/shapes via lookup)
        short_name: r.route_short_name || '',
        long_name: r.route_long_name || '',
        color: r.route_color,
      };
    })
    .filter(r => {
      // de-duplicate by public id
      if (seen.has(r.route_id)) return false;
      seen.add(r.route_id); return true;
    })
    .sort((a, b) => {
      const sa = a.route_id;
      const sb = b.route_id;
      return sa.localeCompare(sb, undefined, { numeric: true, sensitivity: 'base' });
    });
  res.json({ routes: list, total: list.length, loaded: state.gtfsStaticLoaded });
});

// Get the polyline(s) for a specific route. Accepts either the public id
// (e.g. "T807") or the internal GTFS route_id (e.g. "30000125").
app.get('/api/route/:routeId/shapes', (req, res) => {
  let rid = req.params.routeId;
  // If the supplied id is a public id, translate to internal first
  if (!state.routeToShapes[rid] && state.publicIdToInternal && state.publicIdToInternal[rid]) {
    rid = state.publicIdToInternal[rid];
  }
  const shapeIds = state.routeToShapes[rid];
  if (!shapeIds) return res.status(404).json({ error: 'route not found or no shapes' });
  const route = state.routes[rid] || null;
  const polylines = [...shapeIds].map(sid => ({
    shape_id: sid,
    points: state.shapes[sid] || [],
  })).filter(p => p.points.length > 0);
  res.json({ route, polylines });
});

// ─── Rail API (schedule-based, from rapid-rail-kl GTFS static) ─────────────
app.get('/api/rail/lines', (_req, res) => {
  res.json({ lines: railModule.listLines(), loaded: railModule.rail.loaded });
});

app.get('/api/rail/line/:routeId', (req, res) => {
  const data = railModule.lineStations(req.params.routeId);
  if (!data.route) return res.status(404).json({ error: 'line not found' });
  res.json(data);
});

app.get('/api/rail/arrivals/:stopId', (req, res) => {
  const { stopId } = req.params;
  const routeId = req.query.route;
  if (!routeId) return res.status(400).json({ error: 'route query param required' });
  if (!railModule.rail.stops[stopId]) return res.status(404).json({ error: 'stop not found' });
  res.json(railModule.nextTrains(routeId, stopId, new Date(), 90));
});

// All rail stations (deduped by name) for the journey planner search boxes.
app.get('/api/rail/stations', (_req, res) => {
  res.json({ stations: railModule.allStations(), loaded: railModule.rail.loaded });
});

// Journey planning: from -> to, returns legs + transfers + fare.
app.get('/api/rail/journey', (req, res) => {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from and to query params required' });
  res.json(railModule.planJourney(from, to));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', ws =>
  ws.send(JSON.stringify({ type: 'snapshot', payload: snapshot() })));

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(data);
}

server.listen(PORT, () => {
  console.log(`Rapid KL monitor listening on :${PORT}`);
  console.log(`Bus poll: ${BUS_POLL_MS/1000}s · RSS poll: ${RSS_POLL_MS/1000}s`);
  pollBuses().catch(e => console.error('bus poll error:', e));
  pollRss().catch(e => console.error('rss poll error:', e));
  loadGtfsStatic().catch(e => console.error('[gtfs-static]', e.message));
  railModule.loadRailGtfs().catch(e => console.error('[rail]', e.message));
  setInterval(() => pollBuses().catch(e => console.error('bus poll error:', e)), BUS_POLL_MS);
  setInterval(() => pollRss().catch(e => console.error('rss poll error:', e)), RSS_POLL_MS);
  setInterval(() => loadGtfsStatic().catch(e => console.error('[gtfs-static]', e.message)), GTFS_REFRESH_MS);
  setInterval(() => railModule.loadRailGtfs().catch(e => console.error('[rail]', e.message)), railModule.REFRESH_MS);
});
