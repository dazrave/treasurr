# Changelog

## [0.3.0] - 2026-03-03

### Added

- **Walk the Plank** — Content enters a grace period before permanent deletion, giving the crew a chance to save it.
  - **Plank grace period** — Configurable days (default 14) before content is actually deleted. Set to 0 for instant delete (legacy behaviour).
  - **Anchored mode** — Planked content stays on owner's quota. Only the owner can rescue it (undo their delete).
  - **Adrift mode** — Planked content floats off the owner's quota. Any crew member who watches it auto-rescues it.
  - **Rescue actions** — When adrift content is rescued: promote to shared plunder, or adopt onto the rescuer's quota.
  - **Walking the Plank section** — Visible to all users on the dashboard, shows countdown timers and rescue buttons.
  - **Auto-scuttle integration** — Retention engine puts content on the plank instead of instant-deleting.
  - **Plank checks in scheduler** — Expired plank content is automatically deleted; watched adrift content is auto-rescued.
- New API endpoints: `GET /api/plank`, `POST /api/treasure/{id}/rescue`
- Admin plank settings: plank_mode, plank_days, plank_rescue_action
- Plank stats in admin panel (bytes and count)
- 29 new plank tests (125 total)

### Changed

- Scuttle flow now checks plank_days setting before deleting — planks content when > 0.
- `ScuttleResult` and `RescueResult` models moved to `models.py` for shared use.
- `get_quota_summary()` now accepts `plank_mode` parameter for anchored/adrift quota calculation.
- Scuttle confirmation modal shows plank period info when applicable.
- Admin settings panel includes Walk the Plank configuration.
- Scheduler runs plank checks after retention checks.

### Schema

- New column: `content_ownership.plank_started_at` (TEXT, nullable)
- Updated CHECK constraint: `content_ownership.status` now allows `'plank'`
- New settings keys: `plank_mode`, `plank_days`, `plank_rescue_action`

## [0.2.0] - 2026-03-03

### Added

- **Promotion Modes** — Admin can switch between "Full Plunder" (shared content is free for everyone) and "Split the Loot" (shared content is split equally between all viewers). Switchable at runtime via admin panel.
- **Shared Plunder Cap** — Optional max size for the shared pool. When the cap is hit, no new content is promoted until space is freed. Warning banner at 90% capacity.
- **User Retention Policies** — Users can set auto-cleanup timers (7/14/30/60/90 days after watching). Content is automatically deleted when the timer expires.
- **Admin Minimum Retention** — Admin sets a floor: content must exist for at least X days before auto-delete can remove it. Both conditions (user timer + admin minimum) must be met.
- **Runtime Settings System** — New `settings` table allows admin to change promotion mode, plunder cap, retention floor, and display mode without restarting the server.
- **Display Modes** — Admin toggle for how sizes are shown to users: exact (23.4 GB), rounded up (24 GB), or as a percentage of their space.
- **Quality Labels** — Content items now show quality badges (4K, 1080p HD, 720p HD, SD) so users understand why some content uses more space.
- **Onboarding Wizard** — First-time users see a step-by-step guide explaining how storage, sharing, quality, and auto-cleanup work. Plain English, no jargon.
- **Quota Splits Tracking** — New `quota_splits` table tracks per-user share when in "Split the Loot" mode.

### Changed

- Quota calculation now includes split shares when in split mode.
- Promotion engine checks plunder cap before each promotion.
- Deletion engine cleans up quota splits when content is scuttled.
- Sync scheduler now runs retention checks after promotions.
- Stats endpoint includes plunder cap info and warning flags.
- Treasure endpoint includes auto-scuttle, retention, display mode, and split info.

### API Changes

- `GET /api/admin/settings` — Current settings with defaults
- `PUT /api/admin/settings` — Update promotion_mode, shared_plunder_max_bytes, min_retention_days, display_mode
- `PUT /api/treasure/retention` — Set user's auto-cleanup timer
- `POST /api/treasure/onboarded` — Mark user as having completed onboarding
- `GET /api/admin/stats` — Now includes plunder cap info + warning flags
- `GET /api/treasure` — Now includes auto_scuttle_days, min_retention_days, display_mode, promotion_mode, split_bytes, onboarded
- `GET /api/treasure/chest` — Now includes quality and quality_note per item

### Schema

- New table: `settings` (key-value store for runtime config)
- New table: `quota_splits` (per-user share tracking for split mode)
- New columns on `users`: `auto_scuttle_days`, `onboarded`

## [0.1.0] - 2026-03-02

### Added

- Initial MVP release
- Plex OAuth authentication
- Content ownership tracking via Overseerr requests
- Watch event sync from Tautulli
- Automatic promotion when 2+ non-owner viewers watch content
- Quota calculation (owned content counts against user's space)
- Scuttle (delete) workflow with rate limiting
- Admin panel with crew management, stats, and manual sync
- User dashboard with quota bar, cargo list, and activity feed
- Background sync scheduler (Tautulli, Overseerr, Sonarr, Radarr)
- Docker deployment support
- 57 tests passing
