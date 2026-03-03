```
 _____
|_   _| __ ___  __ _ ___ _   _ _ __ _ __
  | || '__/ _ \/ _` / __| | | | '__| '__|
  | || | |  __/ (_| \__ \ |_| | |  | |
  |_||_|  \___|\__,_|___/\__,_|_|  |_|

  Your treasure. Your crew. Your plunder.
```

# Treasurr

**A fair-use storage manager for shared Plex servers.**

## The Problem

Running a Plex server for friends and family is great until someone requests 47 seasons of reality TV and your drives are full. You can't just delete stuff without upsetting people, and you definitely can't keep buying more storage forever.

Most server admins end up being the bad guy, manually deciding what stays and what goes. That doesn't scale, and it's not fun.

## The Solution

Treasurr gives every user a virtual storage quota (a "treasure chest"). When they request content through Overseerr, it counts against their space. If they're running low, they need to clean up their own stuff before requesting more.

The clever bit: when other people actually watch something, it gets **promoted to shared plunder** and stops counting against the requester's quota. So content that the whole crew enjoys is essentially free. Content that only one person watches stays on their tab.

It's crowdsourced storage management. The server runs itself.

## How It Works

```
User requests a film via Overseerr
        |
        v
Treasurr assigns it to their quota (500 GB default)
        |
        v
Other users watch it via Plex (tracked by Tautulli)
        |
        v
2+ crew members finish it? --> Promoted to shared plunder (free for everyone)
        |
Only the requester watched it? --> Still on their quota
        |
        v
User running low on space? --> They scuttle (delete) their own content
```

Nobody argues about what to delete. Everyone manages their own space. Popular content stays forever. Niche stuff is the requester's responsibility.

## Features

### Quota System
- Each user gets a configurable amount of storage (default 500 GB)
- Admins can set custom quotas, bonus space, and tiered plans
- Content size pulled directly from Sonarr/Radarr so the numbers are accurate

### Automatic Promotion
- When enough crew members watch something (default: 2 unique viewers), it becomes shared
- Two modes: **Full Plunder** (shared content is free for everyone) or **Split the Loot** (split equally between viewers)
- Optional cap on total shared storage to prevent runaway growth

### Walk the Plank
- When someone deletes content, it doesn't vanish immediately
- It enters a grace period (default 14 days) where the crew can save it
- **Anchored mode** - stays on the owner's quota, only they can undo the delete
- **Adrift mode** - floats free, anyone who watches it rescues it automatically
- Prevents the "I was about to watch that!" problem

### Auto-Cleanup
- Users can set their own retention timer (7, 14, 30, 60, or 90 days after watching)
- Content automatically cleans itself up after they're done with it
- Admin can set a minimum retention floor so nothing disappears too quickly

### User Dashboard
- Clean dark UI showing quota usage, owned content, and shared plunder
- Quality badges (4K, 1080p, 720p, SD) so users understand why some content uses more space
- Activity feed showing promotions and deletions
- First-time onboarding walkthrough

### Admin Panel
- Crew management with per-user quota controls
- Global stats (total storage, owned, shared, planked)
- Runtime settings that apply immediately without restart
- Manual sync trigger

## Stack

- **Backend:** Python 3.12+, FastAPI, SQLite (WAL mode)
- **Frontend:** Vanilla HTML/CSS/JS (no build step, no frameworks)
- **Integrations:** Tautulli, Overseerr/Jellyseerr, Sonarr, Radarr, Plex OAuth
- **Deployment:** Docker

## Quick Start

```bash
# Clone and configure
git clone https://github.com/dazrave/treasurr.git
cd treasurr
cp config.example.yaml config.yaml
cp .env.example .env

# Edit config.yaml with your API URLs
# Edit .env with your API keys and secrets

# Run
docker compose up -d
```

Then open `http://your-server:8080` and sign in with Plex.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TREASURR_SECRET_KEY` | Session encryption key |
| `TREASURR_PLEX_CLIENT_ID` | Plex OAuth client ID |
| `TREASURR_TAUTULLI_KEY` | Tautulli API key |
| `TREASURR_OVERSEERR_KEY` | Overseerr/Jellyseerr API key |
| `TREASURR_SONARR_KEY` | Sonarr API key |
| `TREASURR_RADARR_KEY` | Radarr API key |

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest                                    # 125 tests
python -m treasurr serve --config config.example.yaml
```

## Configuration

All settings can be configured in `config.yaml` and most can be changed at runtime through the admin panel.

See [`config.example.yaml`](config.example.yaml) for all available options with comments.

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth/plex` | GET | Start Plex OAuth flow |
| `/api/auth/plex/callback` | POST | Complete Plex OAuth |
| `/api/auth/me` | GET | Current user info |
| `/api/treasure` | GET | Quota summary |
| `/api/treasure/chest` | GET | User's owned content |
| `/api/treasure/{id}/scuttle` | POST | Delete owned content |
| `/api/treasure/{id}/rescue` | POST | Rescue content from plank |
| `/api/plank` | GET | Content walking the plank |
| `/api/plunder` | GET | Shared plunder list |
| `/api/activity` | GET | Recent activity feed |
| `/api/treasure/retention` | PUT | Set auto-cleanup timer |
| `/api/admin/crew` | GET | All users (admin) |
| `/api/admin/crew/{id}` | PUT | Update user quota (admin) |
| `/api/admin/settings` | GET/PUT | Server settings (admin) |
| `/api/admin/stats` | GET | Global storage stats (admin) |
| `/api/admin/sync` | POST | Trigger manual sync (admin) |

## License

MIT
