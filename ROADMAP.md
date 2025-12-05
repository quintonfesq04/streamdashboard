# StreamDashboard — vNext Roadmap & Issue List

## 🎯 High-Level Goal

Turn StreamDashboard from a basic scoreboard/watchlist aggregator into a full-featured, engaging sports-fan hub that combines real-time data, fan interactivity, personalization, multimedia, and scalable infrastructure.

Below is a list of major feature improvements and enhancements to implement in upcoming versions. Each item can become a dedicated tracker issue.

---

## 🧩 Issues / Enhancement Tasks

### 1. Shared Real-Time Fan Chat (per-game / global)

**Description**
- Add a WebSocket server layer (e.g., `ws`) on top of the existing Express backend.
- Maintain per-game/global chat stores in memory and persist them to `data/chat.json`.
- Frontend: connect to the WebSocket whenever the Fan Zone is active for a game; subscribe to updates.
- Provide an HTTP polling fallback (`/api/chat?gameId=...`) every ~10 s when WS is unavailable.
- Sanitize/limit incoming messages (basic profanity filter + rate limiting); send error feedback to clients.

**Acceptance Criteria**
- Multiple users/devices see the same real-time chat history.
- When WebSocket fails or is unsupported, polling kicks in and the UI informs the user.
- Chats survive server restarts via JSON persistence.
- Spam/profanity attempts are blocked with clear client errors.

---

### 2. Polls & Match-Time Fan Voting (per game)

**Description**
- Maintain `data/polls.json` keyed by game ID: question, options (with `id/label/count`), votes map, closed flag.
- REST endpoints:
  - `GET /api/polls/:gameId` → poll state.
  - `POST /api/polls/:gameId/vote` → cast votes (dedupe per user/IP).
- Broadcast poll updates over WebSocket to live clients.
- Frontend Fan Zone shows options, live percentages, disables voting post-submit or when closed, shows final results.

**Acceptance Criteria**
- Poll UI appears only when polls exist; otherwise hides gracefully.
- Votes persist and reload accurately.
- Duplicate votes blocked (per user/IP).
- Network failures show “vote failed” messaging instead of silently failing.

---

### 3. Game “Excitement Score” & Enhanced Discovery/Highlighting Logic

**Description**
- During schedule fetch, compute `excitementScore` using factors like score differential, time remaining, scoring run, rivalry flags, user favorites, etc.
- Include the score in `/api/schedule` responses or expose `/api/excitement/trending`.
- Frontend uses the score to:
  - Tag hot games in the Live Ticker / score cards.
  - Order Discovery rail and Smart Multicast suggestions.
  - Provide user preference toggles (favorites bias vs. excitement vs. blended).

**Acceptance Criteria**
- Live games update to reflect changing excitement.
- Pre-game defaults to a neutral score (no “Hot” badge).
- Preference toggles reorder Discovery content accordingly.

---

### 4. Team & Player Profiles + Stats Database & UI Integration

**Description**
- Cache team/player data in `data/teams.json` + `data/players.json`.
- Backend routes: `GET /api/teams/:id`, `/api/teams/:id/stats`, `/api/players/:id`, etc.
- Allow favoriting teams/players (extend favorites schema).
- Frontend: make team/player names clickable; show modal/drawer with overview, roster, stats, highlights, social feed. Degrade gracefully if data missing.

**Acceptance Criteria**
- Clicking team/player loads the profile drawer without error.
- Favorites persist locally/server-side and influence Discovery.
- Absent data results in “Stats pending” instead of broken UI.

---

### 5. Social Sharing, Embedded Social Posts & Co-Watch Skeleton

**Description**
- Backend: `/api/social` proxy for tweets/IG posts; `/api/watchrooms` CRUD to create/join “watch-party” rooms with synced tray + chat (reuse WS).
- Frontend: add “Share” buttons (use `navigator.share` where possible, fallback to clipboard). Display social feeds in drawers when available. Provide Create/Join Watchroom modal; if WS unavailable, fallback to local tray/chat.

**Acceptance Criteria**
- Share links encode enough context (game/room) and open correctly.
- Watchrooms sync tray + chat among participants (when WS available).
- Social embeds render if content exists; otherwise sections hide without breaking layout.

---

### 6. Performance Optimization & Mobile/Responsive UX Improvements

**Description**
- Add responsive CSS breakpoints: ticker collapses, chat/panels stack on small screens, no horizontal scrolling.
- Lazy-load heavy sections via `IntersectionObserver`; ensure fallbacks for unsupported browsers.
- Add accessibility controls: theme toggle (light/dark), font-size slider, high-contrast option stored in `localStorage`.
- Backend: enable gzip/Brotli + cache headers for static assets.

**Acceptance Criteria**
- Mobile/tablet layouts remain usable (no overflow).
- Lazy-loaded sections appear as they enter view; initial load times improve.
- Theme/font settings persist.

---

### 7. Simple Analytics & Metrics Layer (Usage Tracking)

**Description**
- Backend: event logging middleware writing to `data/analytics.json`; aggregated metrics endpoint (`GET /api/admin/metrics`) for top games, chat usage, poll participation, etc.
- Frontend: `track(eventType, payload)` helper that POSTs to `/api/analytics`. Provide opt-out toggle in settings.
- Optionally expose an internal “Insights” panel for admins to view aggregated stats.

**Acceptance Criteria**
- Events log reliably; file grows with usage.
- Opt-out prevents tracking.
- Metrics endpoint returns aggregated counts for dashboards.

---

## 📆 Suggested Implementation Phases

| Phase | Features |
| --- | --- |
| Phase 1 | Shared real-time chat + polls (#1–#2) |
| Phase 2 | Excitement scoring & Discovery upgrades (#3) |
| Phase 3 | Team/player profiles & stats (#4) |
| Phase 4 | Social sharing/embeds + co-watch skeleton (#5) |
| Phase 5 | Performance & responsive UX (#6) |
| Phase 6 | Analytics/metrics layer (#7) |

---

## 📝 Process Notes

- Keep requirements in each issue’s primary description; include acceptance criteria for easier QA.
- Use consistent labels (feature, backend, frontend, enhancement, performance) to make filtering easy.
- Document major UI/UX changes (wireframes or quick sketches) before implementation.
- Keep README/ROADMAP in sync as features land.

---

> **Need an Issue Template?**  
> If you’d like a reusable Markdown template (e.g., `.github/ISSUE_TEMPLATE.md`) for future tasks, mention it and we can scaffold one so every issue captures title, context, acceptance criteria, and rollout notes.
