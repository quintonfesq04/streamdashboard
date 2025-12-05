## StreamDashboard

StreamDashboard is an Express-powered SPA that aggregates live scores, discovery rails, chat, polls, predictions, videos, and watch tools into a single fan-focused sports hub. The project now runs with Redis-backed real-time features, a provider factory abstraction, and an optional job queue for background tasks.

---

## Getting Started

1. **Install dependencies**
   ```bash
   npm install
   ```
2. **Start Redis** (required for shared chat, polls, caching, and queues)
   ```bash
   redis-server
   ```
   The app uses `redis://localhost:6379` by default. Override with `REDIS_URL`.
3. **Run the backend + SPA**
   ```bash
   npm run dev:server
   ```
4. **Visit the app** at <http://localhost:5050>.

### Optional queue worker

- Requires Node 18+ (BullMQ).
- Queue names must be plain strings (no `:`). Defaults to `streamdashboard-queue`.
- If Redis or the queue library is unavailable, the server logs `[queue] Disabled (worker unavailable)` and falls back to synchronous processing.

---

## Core Systems

### Provider factory & caching
- Providers live in `server.js` and expose `getSchedule`, `getGameDetails`, `getTeam`, `getPlayer`.
- `providerFactories` is declared before `initDataProvider`. ESPN provider is default and automatically used if a custom provider fails.
- Redis caching layer stores schedule/news snapshots, chat history, poll state, and job buffers. If Redis is down, JSON file fallbacks keep features working (without multi-user sync).

### Real-time Fan Zone
- Shared chat rooms per game with WebSocket broadcast + REST polling fallback.
- Messages are sanitized, rate-limited, persisted (Redis + JSON dump), and visible across sessions/devices.

### Server-backed polls
- `/api/polls/:gameId` (GET) returns current poll state.
- `/api/polls/:gameId/vote` (POST) records a vote; deduplicated per user/guest.
- WebSocket or polling pushes updates to active clients.

### Excitement scoring
- `/api/schedule` now adds `excitementScore` for every game using score differential, time remaining, scoring runs, rivalry tags, and favorite-team bias.
- Frontend highlights “🔥 Hot” games in Live Ticker, Discovery rail, and Smart Multicast with user preferences for “Favorites vs Hot vs Mixed”.

### Team & player profiles
- Cached under `data/teams.json` / `data/players.json`.
- `/api/teams/:id`, `/api/players/:id`, `/api/teams/:id/stats` load ESPN roster + stats and store them for reuse.
- UI drawers show bios, rosters, stats, social embeds, and “Favorite” toggles. Favorites now accept `{ type:'team'|'player', id }`.

### Predictions engine
- `/api/predictions/:gameId` and `/api/predictions/:gameId` (POST) manage picks.
- `/api/leaderboard` exposes global and league-scoped standings.
- Private prediction leagues allow invite codes, member leaderboards, and rolling winners.

### Multicast & Watch Center
- Multicast tray state is global/persistent; removing games propagates across pages.
- Four-slot layout works at any time, scales to viewport via CSS Grid, and switches to compact modes on small screens.
- Live viewer includes error handling, manual reload button, and stream-overlay messaging for browser autoplay blocks.

### Social & media layers
- `/api/social/posts` proxies relevant tweets/posts by combining team names, hashtags, and timestamps.
- Team/game drawers expose social feeds + fan submissions, plus share buttons (uses `navigator.share` fallback to copy link).
- `/api/videos/:gameId` and `/api/videos/latest` serve highlights/replays for the Videos tab.

### Timeline & insights
- Multi-day horizontal Sports Timeline derived from normalized game data (with highlight moments & scoring events).
- Live Insights panel shows per-team stats, player leaders, drive charts, NBA shot chart, MLB live diamond, and NFL drive visualization.

### Additional modules
- AI recap generator storing summaries in `data/recaps.json`.
- Power Rankings service (`data/elo.json`) with cross-sport “who’s hot” list.
- Historical season explorer via `/api/history/:league/:year`.
- Creator Marketplace, Fan Clips rail, Director Mode, TV Mode (`?tv=1`), Device Sync pairing, premium subscription upsell, and more—all wired to the server state.

---

## Troubleshooting

| Issue | Fix |
| --- | --- |
| **Redis connection refused / chat disabled** | Start `redis-server` (or point `REDIS_URL` to a reachable instance). The server will log `[redis] Connected` when ready. Without Redis, chat/polls run locally only. |
| **Warning: providerFactories undefined / provider not found** | Ensure `server.js` still declares provider functions before `providerFactories`, and `provider.config.json` uses a valid id (`espn`, `fallback`). Unknown ids fall back to ESPN but log `console.warn`. |
| **`/api/schedule` returns 0 games / best matchup null** | The provider may have failed or returned empty data. Check server logs for `[schedule] 0 games` warnings. The API automatically retries ESPN; if still empty, confirm your network connectivity and ESPN endpoints. |
| **UI buttons/tabs unresponsive** | Clear browser cache/localStorage, reload to rebuild selectors. Confirm `window.addEventListener('DOMContentLoaded', initializeApp)` still wraps bootstrap logic. If you edited layout, update selectors used in `attachNavListeners`, `attachCardListeners`, etc. |
| **Queue warning: “Queue name cannot contain :”** | Update `.env` or code to use a safe queue name (default `streamdashboard-queue`). The server already guards this—if you override, avoid `:` characters. |
| **Hero / best matchup still blank** | Verify `/api/schedule` returns `games` array with `id`, `league`, `status`, `excitementScore`. Frontend falls back to first live/upcoming game, but if fields are missing, update the provider normalization. |

---

## Architecture Notes

- **Express server** exposing SPA and API endpoints.
- **Provider abstraction** selects data source via `DATA_PROVIDER` env or `provider.config.json`. ESPN is default fallback.
- **Redis** powers caching, chat rooms, poll state, queue persistence, and analytics buffering. Safe to disable for solo use (features degrade gracefully).
- **BullMQ queue** (optional) handles recap generation, analytics rollups, clip indexing; if disabled, tasks run synchronously.
- **Vanilla JS SPA** lives in `index.html` with modular render functions, state management, and service calls.
- **Data storage** uses `/data/*.json` for user accounts, favorites, recaps, predictions, power rankings, history, creator content, etc.

---

## Contributing / Next Steps

- Keep `providerFactories` and `initDataProvider` ordering intact when adding providers.
- When introducing new UI sections, ensure initialization happens after `DOMContentLoaded` and selectors exist before attaching listeners.
- Use Redis-backed channels for any new real-time features (co-watch rooms, director mode sync, etc.).
- Update this README and `ROADMAP.md` whenever the startup flow or infrastructure prerequisites change.
