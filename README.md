# Rapid KL Operations Monitor

A realtime dashboard for Rapid KL rail (LRT Ampang, LRT Sri Petaling, LRT Kelana Jaya, MRT Kajang, MRT Putrajaya, KL Monorail) and BRT Sunway. Pulls live vehicle positions and trip-update delays from Prasarana's GTFS-Realtime feed on data.gov.my, detects anomalies, and pushes updates to the browser over WebSocket.

## Features
- **Network status board** — every line, its operational status, train count in service, number of delayed trips, worst delay
- **Live alerts** — auto-detected status transitions (normal → delayed → disrupted → recovered)
- **Disruption modal + sound + browser notification** when a line enters disrupted state
- **WebSocket push** so multiple clients stay in sync without hammering the upstream API

## Run locally
```bash
npm install
npm start
# open http://localhost:3000
```
Requires Node.js 20+.

## Configuration
- `PORT` — HTTP port (default 3000)
- Polling interval and thresholds are constants at the top of `server.js`
- Line definitions and route-id matching live in the `LINES` array — adjust `routeMatch` prefixes if the live feed uses different IDs than expected

## How it works
1. Every 15 seconds the backend fetches two protobuf feeds from `api.data.gov.my`:
   - `gtfs-realtime/vehicle-position/prasarana?category=rapid-rail-kl`
   - `gtfs-realtime/trip-updates/prasarana?category=rapid-rail-kl`
2. Decodes with `gtfs-realtime-bindings`, buckets entities by line via `route_id` prefix.
3. Computes status per line:
   - `disrupted` — worst delay ≥ 10 min
   - `delayed` — any trip delay ≥ 5 min
   - `no-data` — zero vehicles reporting (possible suspension)
   - `normal` — otherwise
4. On status transitions, generates alerts (deduplicated within 10 min).
5. Broadcasts the full snapshot to all WebSocket clients.

## Deploy

### Railway / Render / Fly.io (recommended — all have free tiers)
1. Push this folder to GitHub.
2. On Railway: New Project → Deploy from GitHub → pick the repo. It auto-detects Node and runs `npm start`. Expose port 3000.
3. Same flow on Render (Web Service) or Fly.io (`fly launch`).

### Docker
```Dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
```

## Tuning the alert sensitivity
In `server.js`:
- `DELAY_THRESHOLD_SEC = 5*60` — what counts as a "delayed" trip
- A line is `disrupted` when max delay ≥ 2× that threshold (10 min)
- `STALE_THRESHOLD_MS` is reserved for a future per-vehicle "stuck train" detector

## Caveats
- Route-id matching is heuristic. If a line shows `no-data` permanently, check the actual `route_id` values in the live feed and update `LINES[i].routeMatch`.
- The BRT Sunway feed historically appeared under different category names — if it's not populating, try also polling `category=rapid-bus-mrtfeeder` or check the current category list at https://developer.data.gov.my/realtime-api/gtfs-realtime
- This dashboard observes the feed; it does **not** verify against official Rapid KL service announcements. A line marked "disrupted" here means the data shows delays, not necessarily that Rapid KL has declared a disruption.
- data.gov.my is rate-limited — keep `POLL_INTERVAL_MS` ≥ 10s.

## License
MIT — do whatever, no warranty.
