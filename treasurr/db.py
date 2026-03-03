"""SQLite database schema, connection management, and repository."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from treasurr.models import (
    Content,
    ContentOwnership,
    DeletionRecord,
    OwnedContent,
    PromotionRecord,
    QuotaSplit,
    QuotaSummary,
    QuotaTransaction,
    Season,
    User,
    WatchEvent,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plex_user_id TEXT UNIQUE NOT NULL,
    plex_username TEXT NOT NULL,
    email TEXT DEFAULT '',
    quota_bytes INTEGER NOT NULL,
    bonus_bytes INTEGER DEFAULT 0,
    is_admin BOOLEAN DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    media_type TEXT NOT NULL CHECK(media_type IN ('movie', 'show')),
    tmdb_id INTEGER NOT NULL,
    sonarr_id INTEGER,
    radarr_id INTEGER,
    overseerr_request_id INTEGER,
    size_bytes INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active' CHECK(status IN ('active', 'deleting', 'deleted')),
    added_at TEXT NOT NULL,
    poster_path TEXT,
    UNIQUE(tmdb_id, media_type)
);

CREATE TABLE IF NOT EXISTS seasons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content(id),
    season_number INTEGER NOT NULL,
    episode_count INTEGER DEFAULT 0,
    size_bytes INTEGER DEFAULT 0,
    UNIQUE(content_id, season_number)
);

CREATE TABLE IF NOT EXISTS content_ownership (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL UNIQUE,
    owner_user_id INTEGER NOT NULL,
    status TEXT DEFAULT 'owned' CHECK(status IN ('owned', 'promoted', 'released', 'plank', 'buried')),
    owned_at TEXT NOT NULL,
    promoted_at TEXT,
    FOREIGN KEY (content_id) REFERENCES content(id),
    FOREIGN KEY (owner_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS watch_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    watched_at TEXT NOT NULL,
    completed BOOLEAN DEFAULT 0,
    UNIQUE(content_id, user_id, watched_at),
    FOREIGN KEY (content_id) REFERENCES content(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS promotion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL,
    from_user_id INTEGER NOT NULL,
    unique_viewers INTEGER NOT NULL,
    size_freed_bytes INTEGER NOT NULL,
    promoted_at TEXT NOT NULL,
    FOREIGN KEY (content_id) REFERENCES content(id),
    FOREIGN KEY (from_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS deletion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL,
    deleted_by_user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    deleted_at TEXT NOT NULL,
    FOREIGN KEY (content_id) REFERENCES content(id),
    FOREIGN KEY (deleted_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS quota_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    change_bytes INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    plex_token TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_splits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    share_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(content_id, user_id),
    FOREIGN KEY (content_id) REFERENCES content(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """SQLite database with WAL mode and repository methods."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self.connection() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Add columns/tables that may not exist in older databases."""
        columns = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "auto_scuttle_days" not in columns:
            conn.execute("ALTER TABLE users ADD COLUMN auto_scuttle_days INTEGER DEFAULT 0")
        if "onboarded" not in columns:
            conn.execute("ALTER TABLE users ADD COLUMN onboarded BOOLEAN DEFAULT 0")

        content_cols = {row[1] for row in conn.execute("PRAGMA table_info(content)").fetchall()}
        if "poster_path" not in content_cols:
            conn.execute("ALTER TABLE content ADD COLUMN poster_path TEXT")

        ownership_cols = {row[1] for row in conn.execute("PRAGMA table_info(content_ownership)").fetchall()}
        if "plank_started_at" not in ownership_cols:
            # Recreate content_ownership table to update CHECK constraint and add plank_started_at
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS content_ownership_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL UNIQUE,
                    owner_user_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'owned' CHECK(status IN ('owned', 'promoted', 'released', 'plank', 'buried')),
                    owned_at TEXT NOT NULL,
                    promoted_at TEXT,
                    plank_started_at TEXT,
                    buried_at TEXT,
                    FOREIGN KEY (content_id) REFERENCES content(id),
                    FOREIGN KEY (owner_user_id) REFERENCES users(id)
                );
                INSERT INTO content_ownership_new (id, content_id, owner_user_id, status, owned_at, promoted_at)
                    SELECT id, content_id, owner_user_id, status, owned_at, promoted_at FROM content_ownership;
                DROP TABLE content_ownership;
                ALTER TABLE content_ownership_new RENAME TO content_ownership;
            """)
        if "buried_at" not in ownership_cols and "plank_started_at" in ownership_cols:
            # Add buried_at and update CHECK constraint for existing DBs that already have plank_started_at
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS content_ownership_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL UNIQUE,
                    owner_user_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'owned' CHECK(status IN ('owned', 'promoted', 'released', 'plank', 'buried')),
                    owned_at TEXT NOT NULL,
                    promoted_at TEXT,
                    plank_started_at TEXT,
                    buried_at TEXT,
                    FOREIGN KEY (content_id) REFERENCES content(id),
                    FOREIGN KEY (owner_user_id) REFERENCES users(id)
                );
                INSERT INTO content_ownership_new (id, content_id, owner_user_id, status, owned_at, promoted_at, plank_started_at)
                    SELECT id, content_id, owner_user_id, status, owned_at, promoted_at, plank_started_at FROM content_ownership;
                DROP TABLE content_ownership;
                ALTER TABLE content_ownership_new RENAME TO content_ownership;
            """)

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # --- Users ---

    def upsert_user(
        self,
        plex_user_id: str,
        plex_username: str,
        email: str = "",
        quota_bytes: int = 536_870_912_000,
        is_admin: bool = False,
    ) -> User:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO users (plex_user_id, plex_username, email, quota_bytes, is_admin, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(plex_user_id) DO UPDATE SET
                     plex_username = excluded.plex_username,
                     email = CASE WHEN excluded.email != '' THEN excluded.email ELSE users.email END""",
                (plex_user_id, plex_username, email, quota_bytes, is_admin, _now()),
            )
            row = conn.execute(
                "SELECT * FROM users WHERE plex_user_id = ?", (plex_user_id,)
            ).fetchone()
            return _row_to_user(row)

    def get_user(self, user_id: int) -> User | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            return _row_to_user(row) if row else None

    def get_user_by_plex_id(self, plex_user_id: str) -> User | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE plex_user_id = ?", (plex_user_id,)
            ).fetchone()
            return _row_to_user(row) if row else None

    def get_user_by_username(self, username: str) -> User | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE plex_username = ?", (username,)
            ).fetchone()
            return _row_to_user(row) if row else None

    def get_all_users(self) -> list[User]:
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM users ORDER BY plex_username").fetchall()
            return [_row_to_user(r) for r in rows]

    def update_user_quota(self, user_id: int, quota_bytes: int | None = None, bonus_bytes: int | None = None) -> User | None:
        with self.connection() as conn:
            if quota_bytes is not None:
                conn.execute("UPDATE users SET quota_bytes = ? WHERE id = ?", (quota_bytes, user_id))
            if bonus_bytes is not None:
                conn.execute("UPDATE users SET bonus_bytes = ? WHERE id = ?", (bonus_bytes, user_id))
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            return _row_to_user(row) if row else None

    def bulk_update_quota(self, user_ids: list[int], quota_bytes: int) -> int:
        """Update quota for multiple users at once. Returns count of updated users."""
        if not user_ids:
            return 0
        placeholders = ",".join("?" * len(user_ids))
        now = _now()
        with self.connection() as conn:
            conn.execute(
                f"UPDATE users SET quota_bytes = ? WHERE id IN ({placeholders})",
                [quota_bytes, *user_ids],
            )
            for uid in user_ids:
                conn.execute(
                    "INSERT INTO quota_transactions (user_id, change_bytes, reason, created_at) VALUES (?, ?, ?, ?)",
                    (uid, quota_bytes, "admin_bulk_grant", now),
                )
        return len(user_ids)

    def get_user_activity(self) -> dict[int, dict]:
        """Get last request date and request count per user.

        Returns {user_id: {"last_request_at": str|None, "request_count": int}}.
        """
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT co.owner_user_id as user_id,
                          MAX(co.owned_at) as last_request_at,
                          COUNT(co.id) as request_count
                   FROM content_ownership co
                   GROUP BY co.owner_user_id"""
            ).fetchall()
            return {
                row["user_id"]: {
                    "last_request_at": row["last_request_at"],
                    "request_count": row["request_count"],
                }
                for row in rows
            }

    # --- Content ---

    def upsert_content(
        self,
        title: str,
        media_type: str,
        tmdb_id: int,
        sonarr_id: int | None = None,
        radarr_id: int | None = None,
        overseerr_request_id: int | None = None,
        size_bytes: int = 0,
    ) -> Content:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO content (title, media_type, tmdb_id, sonarr_id, radarr_id,
                                        overseerr_request_id, size_bytes, added_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tmdb_id, media_type) DO UPDATE SET
                     title = excluded.title,
                     sonarr_id = COALESCE(excluded.sonarr_id, content.sonarr_id),
                     radarr_id = COALESCE(excluded.radarr_id, content.radarr_id),
                     overseerr_request_id = COALESCE(excluded.overseerr_request_id, content.overseerr_request_id),
                     size_bytes = CASE WHEN excluded.size_bytes > 0 THEN excluded.size_bytes ELSE content.size_bytes END""",
                (title, media_type, tmdb_id, sonarr_id, radarr_id, overseerr_request_id, size_bytes, _now()),
            )
            row = conn.execute(
                "SELECT * FROM content WHERE tmdb_id = ? AND media_type = ?", (tmdb_id, media_type)
            ).fetchone()
            return _row_to_content(row)

    def get_content(self, content_id: int) -> Content | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM content WHERE id = ?", (content_id,)).fetchone()
            return _row_to_content(row) if row else None

    def get_content_by_tmdb(self, tmdb_id: int, media_type: str) -> Content | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM content WHERE tmdb_id = ? AND media_type = ?",
                (tmdb_id, media_type),
            ).fetchone()
            return _row_to_content(row) if row else None

    def update_content_size(self, content_id: int, size_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE content SET size_bytes = ? WHERE id = ?", (size_bytes, content_id))

    def update_content_title(self, content_id: int, title: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE content SET title = ? WHERE id = ?", (title, content_id))

    def update_content_status(self, content_id: int, status: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE content SET status = ? WHERE id = ?", (status, content_id))

    def update_content_arr_ids(self, content_id: int, sonarr_id: int | None = None, radarr_id: int | None = None) -> None:
        with self.connection() as conn:
            if sonarr_id is not None:
                conn.execute("UPDATE content SET sonarr_id = ? WHERE id = ?", (sonarr_id, content_id))
            if radarr_id is not None:
                conn.execute("UPDATE content SET radarr_id = ? WHERE id = ?", (radarr_id, content_id))

    def update_content_poster(self, content_id: int, poster_path: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE content SET poster_path = ? WHERE id = ?", (poster_path, content_id))

    def get_all_active_content(self) -> list[Content]:
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM content WHERE status = 'active' ORDER BY title").fetchall()
            return [_row_to_content(r) for r in rows]

    # --- Seasons ---

    def upsert_season(self, content_id: int, season_number: int, episode_count: int, size_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO seasons (content_id, season_number, episode_count, size_bytes)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(content_id, season_number) DO UPDATE SET
                     episode_count = excluded.episode_count,
                     size_bytes = excluded.size_bytes""",
                (content_id, season_number, episode_count, size_bytes),
            )

    def get_seasons(self, content_id: int) -> list[Season]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM seasons WHERE content_id = ? ORDER BY season_number",
                (content_id,),
            ).fetchall()
            return [
                Season(
                    id=r["id"],
                    content_id=r["content_id"],
                    season_number=r["season_number"],
                    episode_count=r["episode_count"],
                    size_bytes=r["size_bytes"],
                )
                for r in rows
            ]

    def delete_seasons(self, content_id: int) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM seasons WHERE content_id = ?", (content_id,))

    def update_season_size(self, content_id: int, season_number: int, size_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE seasons SET size_bytes = ? WHERE content_id = ? AND season_number = ?",
                (size_bytes, content_id, season_number),
            )

    # --- Ownership ---

    def set_ownership(self, content_id: int, owner_user_id: int) -> ContentOwnership:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO content_ownership (content_id, owner_user_id, owned_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(content_id) DO UPDATE SET
                     owner_user_id = excluded.owner_user_id""",
                (content_id, owner_user_id, _now()),
            )
            row = conn.execute(
                "SELECT * FROM content_ownership WHERE content_id = ?", (content_id,)
            ).fetchone()
            return _row_to_ownership(row)

    def get_ownership(self, content_id: int) -> ContentOwnership | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM content_ownership WHERE content_id = ?", (content_id,)
            ).fetchone()
            return _row_to_ownership(row) if row else None

    def get_user_owned_content(self, user_id: int) -> list[OwnedContent]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.*, co.id as co_id, co.content_id as co_content_id,
                          co.owner_user_id, co.status as co_status,
                          co.owned_at, co.promoted_at, co.buried_at,
                          (SELECT COUNT(DISTINCT w.user_id)
                           FROM watch_events w
                           WHERE w.content_id = c.id AND w.completed = 1
                             AND w.user_id != co.owner_user_id) as unique_viewers
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.owner_user_id = ? AND c.status = 'active'
                   ORDER BY co.status, c.title""",
                (user_id,),
            ).fetchall()
            return [
                OwnedContent(
                    content=_row_to_content(r),
                    ownership=ContentOwnership(
                        id=r["co_id"],
                        content_id=r["co_content_id"],
                        owner_user_id=r["owner_user_id"],
                        status=r["co_status"],
                        owned_at=r["owned_at"],
                        promoted_at=r["promoted_at"],
                        buried_at=r["buried_at"],
                    ),
                    unique_viewers=r["unique_viewers"],
                )
                for r in rows
            ]

    def get_owned_content_for_promotion(self) -> list[OwnedContent]:
        """Get all owned (not yet promoted) content with viewer counts."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.*, co.id as co_id, co.content_id as co_content_id,
                          co.owner_user_id, co.status as co_status,
                          co.owned_at, co.promoted_at,
                          (SELECT COUNT(DISTINCT w.user_id)
                           FROM watch_events w
                           WHERE w.content_id = c.id AND w.completed = 1
                             AND w.user_id != co.owner_user_id) as unique_viewers
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status IN ('owned', 'buried') AND c.status = 'active'""",
            ).fetchall()
            return [
                OwnedContent(
                    content=_row_to_content(r),
                    ownership=ContentOwnership(
                        id=r["co_id"],
                        content_id=r["co_content_id"],
                        owner_user_id=r["owner_user_id"],
                        status=r["co_status"],
                        owned_at=r["owned_at"],
                        promoted_at=r["promoted_at"],
                    ),
                    unique_viewers=r["unique_viewers"],
                )
                for r in rows
            ]

    def promote_content(self, content_id: int) -> None:
        now = _now()
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'promoted', promoted_at = ? WHERE content_id = ?",
                (now, content_id),
            )

    def release_content(self, content_id: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'released' WHERE content_id = ?",
                (content_id,),
            )

    # --- Watch Events ---

    def add_watch_event(self, content_id: int, user_id: int, watched_at: str, completed: bool) -> None:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO watch_events (content_id, user_id, watched_at, completed)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(content_id, user_id, watched_at) DO UPDATE SET
                     completed = MAX(watch_events.completed, excluded.completed)""",
                (content_id, user_id, watched_at, completed),
            )

    def get_unique_viewers(self, content_id: int, exclude_user_id: int | None = None) -> int:
        with self.connection() as conn:
            if exclude_user_id is not None:
                row = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) as cnt FROM watch_events WHERE content_id = ? AND completed = 1 AND user_id != ?",
                    (content_id, exclude_user_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) as cnt FROM watch_events WHERE content_id = ? AND completed = 1",
                    (content_id,),
                ).fetchone()
            return row["cnt"] if row else 0

    # --- Promotion Log ---

    def log_promotion(self, content_id: int, from_user_id: int, unique_viewers: int, size_freed_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO promotion_log (content_id, from_user_id, unique_viewers, size_freed_bytes, promoted_at) VALUES (?, ?, ?, ?, ?)",
                (content_id, from_user_id, unique_viewers, size_freed_bytes, _now()),
            )

    def get_recent_promotions(self, limit: int = 20) -> list[PromotionRecord]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM promotion_log ORDER BY promoted_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [_row_to_promotion(r) for r in rows]

    # --- Deletion Log ---

    def log_deletion(self, content_id: int, deleted_by_user_id: int, title: str, size_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO deletion_log (content_id, deleted_by_user_id, title, size_bytes, deleted_at) VALUES (?, ?, ?, ?, ?)",
                (content_id, deleted_by_user_id, title, size_bytes, _now()),
            )

    def get_recent_deletions(self, limit: int = 20) -> list[DeletionRecord]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM deletion_log ORDER BY deleted_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [_row_to_deletion(r) for r in rows]

    def count_recent_deletions(self, user_id: int) -> int:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT COUNT(*) as cnt FROM deletion_log
                   WHERE deleted_by_user_id = ?
                   AND deleted_at > datetime('now', '-1 hour')""",
                (user_id,),
            ).fetchone()
            return row["cnt"] if row else 0

    # --- Quota ---

    def get_quota_summary(
        self, user_id: int, include_splits: bool = False, plank_mode: str = "adrift",
    ) -> QuotaSummary | None:
        with self.connection() as conn:
            user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            if not user_row:
                return None

            # Buried always counts. In anchored mode, plank content also counts.
            if plank_mode == "anchored":
                status_filter = "co.status IN ('owned', 'buried', 'plank')"
            else:
                status_filter = "co.status IN ('owned', 'buried')"

            usage_row = conn.execute(
                f"""SELECT COALESCE(SUM(c.size_bytes), 0) as used_bytes,
                          COUNT(c.id) as owned_count
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.owner_user_id = ? AND {status_filter} AND c.status = 'active'""",
                (user_id,),
            ).fetchone()

            split_bytes = 0
            if include_splits:
                split_row = conn.execute(
                    """SELECT COALESCE(SUM(qs.share_bytes), 0) as total
                       FROM quota_splits qs
                       JOIN content c ON c.id = qs.content_id
                       WHERE qs.user_id = ? AND c.status = 'active'""",
                    (user_id,),
                ).fetchone()
                split_bytes = split_row["total"]

            return QuotaSummary(
                user_id=user_id,
                quota_bytes=user_row["quota_bytes"],
                bonus_bytes=user_row["bonus_bytes"],
                used_bytes=usage_row["used_bytes"],
                owned_count=usage_row["owned_count"],
                split_bytes=split_bytes,
            )

    def add_quota_transaction(self, user_id: int, change_bytes: int, reason: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO quota_transactions (user_id, change_bytes, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, change_bytes, reason, _now()),
            )

    # --- Sessions ---

    def create_session(self, token: str, user_id: int, plex_token: str, expires_at: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO sessions (token, user_id, plex_token, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
                (token, user_id, plex_token, _now(), expires_at),
            )

    def get_session(self, token: str) -> dict | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE token = ?", (token,)).fetchone()
            if not row:
                return None
            return dict(row)

    def delete_session(self, token: str) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))

    def cleanup_expired_sessions(self) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM sessions WHERE expires_at < ?", (_now(),))

    # --- Stats ---

    def get_global_stats(self) -> dict:
        with self.connection() as conn:
            total = conn.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) as total FROM content WHERE status = 'active'"
            ).fetchone()["total"]

            owned = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as owned
                   FROM content c JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status IN ('owned', 'buried') AND c.status = 'active'"""
            ).fetchone()["owned"]

            promoted = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as promoted
                   FROM content c JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'promoted' AND c.status = 'active'"""
            ).fetchone()["promoted"]

            unowned = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as unowned
                   FROM content c LEFT JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.id IS NULL AND c.status = 'active'"""
            ).fetchone()["unowned"]

            plank = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as plank_bytes,
                          COUNT(c.id) as plank_count
                   FROM content c JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'plank' AND c.status = 'active'"""
            ).fetchone()

            user_count = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            content_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM content WHERE status = 'active'"
            ).fetchone()["cnt"]

            return {
                "total_bytes": total,
                "owned_bytes": owned,
                "promoted_bytes": promoted,
                "unowned_bytes": unowned,
                "plank_bytes": plank["plank_bytes"],
                "plank_count": plank["plank_count"],
                "user_count": user_count,
                "content_count": content_count,
            }

    # --- Shared Plunder ---

    def get_promoted_content(self) -> list[Content]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.* FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'promoted' AND c.status = 'active'
                   ORDER BY co.promoted_at DESC"""
            ).fetchall()
            return [_row_to_content(r) for r in rows]

    def get_total_promoted_bytes(self) -> int:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as total
                   FROM content c JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'promoted' AND c.status = 'active'"""
            ).fetchone()
            return row["total"]

    # --- Settings ---

    def get_setting(self, key: str, default: str = "") -> str:
        with self.connection() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO settings (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
                (key, value, _now()),
            )

    def get_all_settings(self) -> dict[str, str]:
        with self.connection() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            return {row["key"]: row["value"] for row in rows}

    # --- Quota Splits ---

    def upsert_quota_split(self, content_id: int, user_id: int, share_bytes: int) -> None:
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO quota_splits (content_id, user_id, share_bytes, created_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(content_id, user_id) DO UPDATE SET share_bytes = excluded.share_bytes""",
                (content_id, user_id, share_bytes, _now()),
            )

    def get_user_split_total(self, user_id: int) -> int:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(qs.share_bytes), 0) as total
                   FROM quota_splits qs
                   JOIN content c ON c.id = qs.content_id
                   WHERE qs.user_id = ? AND c.status = 'active'""",
                (user_id,),
            ).fetchone()
            return row["total"]

    def recalculate_splits(self, content_id: int, viewer_ids: list[int], total_bytes: int) -> None:
        if not viewer_ids:
            return
        share = total_bytes // len(viewer_ids)
        with self.connection() as conn:
            # Remove splits for users no longer in viewer list
            conn.execute(
                f"DELETE FROM quota_splits WHERE content_id = ? AND user_id NOT IN ({','.join('?' * len(viewer_ids))})",
                [content_id, *viewer_ids],
            )
            for uid in viewer_ids:
                conn.execute(
                    """INSERT INTO quota_splits (content_id, user_id, share_bytes, created_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(content_id, user_id) DO UPDATE SET share_bytes = excluded.share_bytes""",
                    (content_id, uid, share, _now()),
                )

    def delete_splits_for_content(self, content_id: int) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM quota_splits WHERE content_id = ?", (content_id,))

    def get_all_completed_viewer_ids(self, content_id: int) -> list[int]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT user_id FROM watch_events WHERE content_id = ? AND completed = 1",
                (content_id,),
            ).fetchall()
            return [row["user_id"] for row in rows]

    # --- User Auto-Scuttle ---

    def update_user_auto_scuttle(self, user_id: int, days: int) -> User | None:
        with self.connection() as conn:
            conn.execute("UPDATE users SET auto_scuttle_days = ? WHERE id = ?", (days, user_id))
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            return _row_to_user(row) if row else None

    def update_user_onboarded(self, user_id: int) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE users SET onboarded = 1 WHERE id = ?", (user_id,))

    def get_users_with_auto_scuttle(self) -> list[User]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE auto_scuttle_days > 0"
            ).fetchall()
            return [_row_to_user(r) for r in rows]

    # --- Plank ---

    def plank_content(self, content_id: int) -> None:
        """Move content to the plank (pending deletion)."""
        now = _now()
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'plank', plank_started_at = ? WHERE content_id = ?",
                (now, content_id),
            )

    def rescue_content(self, content_id: int) -> None:
        """Rescue content from the plank back to owned status."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'owned', plank_started_at = NULL WHERE content_id = ?",
                (content_id,),
            )

    def adopt_content(self, content_id: int, new_owner_id: int) -> None:
        """Transfer ownership of planked content to a new user."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET owner_user_id = ?, status = 'owned', plank_started_at = NULL WHERE content_id = ?",
                (new_owner_id, content_id),
            )

    def get_plank_content(self) -> list[OwnedContent]:
        """Get all content currently on the plank."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.*, co.id as co_id, co.content_id as co_content_id,
                          co.owner_user_id, co.status as co_status,
                          co.owned_at, co.promoted_at, co.plank_started_at,
                          (SELECT COUNT(DISTINCT w.user_id)
                           FROM watch_events w
                           WHERE w.content_id = c.id AND w.completed = 1
                             AND w.user_id != co.owner_user_id) as unique_viewers
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'plank' AND c.status = 'active'
                   ORDER BY co.plank_started_at ASC""",
            ).fetchall()
            return [
                OwnedContent(
                    content=_row_to_content(r),
                    ownership=ContentOwnership(
                        id=r["co_id"],
                        content_id=r["co_content_id"],
                        owner_user_id=r["owner_user_id"],
                        status=r["co_status"],
                        owned_at=r["owned_at"],
                        promoted_at=r["promoted_at"],
                        plank_started_at=r["plank_started_at"],
                    ),
                    unique_viewers=r["unique_viewers"],
                )
                for r in rows
            ]

    def get_expired_plank_content(self, plank_days: int) -> list[OwnedContent]:
        """Get plank content where the grace period has expired."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.*, co.id as co_id, co.content_id as co_content_id,
                          co.owner_user_id, co.status as co_status,
                          co.owned_at, co.promoted_at, co.plank_started_at,
                          (SELECT COUNT(DISTINCT w.user_id)
                           FROM watch_events w
                           WHERE w.content_id = c.id AND w.completed = 1
                             AND w.user_id != co.owner_user_id) as unique_viewers
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'plank' AND c.status = 'active'
                     AND julianday('now') - julianday(co.plank_started_at) > ?""",
                (plank_days,),
            ).fetchall()
            return [
                OwnedContent(
                    content=_row_to_content(r),
                    ownership=ContentOwnership(
                        id=r["co_id"],
                        content_id=r["co_content_id"],
                        owner_user_id=r["owner_user_id"],
                        status=r["co_status"],
                        owned_at=r["owned_at"],
                        promoted_at=r["promoted_at"],
                        plank_started_at=r["plank_started_at"],
                    ),
                    unique_viewers=r["unique_viewers"],
                )
                for r in rows
            ]

    def get_user_plank_content(self, user_id: int) -> list[OwnedContent]:
        """Get a specific user's planked content."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.*, co.id as co_id, co.content_id as co_content_id,
                          co.owner_user_id, co.status as co_status,
                          co.owned_at, co.promoted_at, co.plank_started_at,
                          (SELECT COUNT(DISTINCT w.user_id)
                           FROM watch_events w
                           WHERE w.content_id = c.id AND w.completed = 1
                             AND w.user_id != co.owner_user_id) as unique_viewers
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.status = 'plank' AND co.owner_user_id = ? AND c.status = 'active'
                   ORDER BY co.plank_started_at ASC""",
                (user_id,),
            ).fetchall()
            return [
                OwnedContent(
                    content=_row_to_content(r),
                    ownership=ContentOwnership(
                        id=r["co_id"],
                        content_id=r["co_content_id"],
                        owner_user_id=r["owner_user_id"],
                        status=r["co_status"],
                        owned_at=r["owned_at"],
                        promoted_at=r["promoted_at"],
                        plank_started_at=r["plank_started_at"],
                    ),
                    unique_viewers=r["unique_viewers"],
                )
                for r in rows
            ]

    def get_retention_eligible_content(
        self, user_id: int, scuttle_days: int, min_retention_days: int
    ) -> list[Content]:
        """Get content owned by user that is eligible for auto-scuttle.

        Must meet ALL conditions:
        - User owns it (status = 'owned')
        - Content is active
        - User completed watching it
        - Last watch was > scuttle_days ago
        - Content was added > min_retention_days ago
        """
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.* FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   JOIN watch_events w ON w.content_id = c.id AND w.user_id = ?
                   WHERE co.owner_user_id = ?
                     AND co.status = 'owned'
                     AND c.status = 'active'
                     AND w.completed = 1
                     AND julianday('now') - julianday(w.watched_at) > ?
                     AND julianday('now') - julianday(c.added_at) > ?
                   GROUP BY c.id""",
                (user_id, user_id, scuttle_days, min_retention_days),
            ).fetchall()
            return [_row_to_content(r) for r in rows]

    # --- Stale Content (Global Auto-Plank) ---

    def get_stale_content(self, stale_days: int) -> list[Content]:
        """Get active content where last watch by anyone (or added_at if never watched) exceeds stale_days.

        Includes owned (not buried) content and unclaimed content (no ownership row).
        """
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.* FROM content c
                   LEFT JOIN content_ownership co ON co.content_id = c.id
                   LEFT JOIN (
                       SELECT content_id, MAX(watched_at) as last_watched
                       FROM watch_events WHERE completed = 1
                       GROUP BY content_id
                   ) lw ON lw.content_id = c.id
                   WHERE c.status = 'active'
                     AND (co.status = 'owned' OR co.id IS NULL)
                     AND julianday('now') - julianday(COALESCE(lw.last_watched, c.added_at)) > ?
                   ORDER BY COALESCE(lw.last_watched, c.added_at) ASC""",
                (stale_days,),
            ).fetchall()
            return [_row_to_content(r) for r in rows]

    # --- Bury (Protected Content) ---

    def bury_content(self, content_id: int) -> None:
        """Protect content from auto-cleanup by burying it."""
        now = _now()
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'buried', buried_at = ? WHERE content_id = ? AND status = 'owned'",
                (now, content_id),
            )

    def unbury_content(self, content_id: int) -> None:
        """Remove bury protection, return to owned status."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET status = 'owned', buried_at = NULL WHERE content_id = ? AND status = 'buried'",
                (content_id,),
            )

    # --- User-Relevant Promoted Content ---

    def get_relevant_promoted_content(self, user_id: int) -> list[Content]:
        """Get promoted content relevant to a specific user (they own it or watched it)."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT DISTINCT c.* FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   LEFT JOIN watch_events w ON w.content_id = c.id AND w.user_id = ? AND w.completed = 1
                   WHERE co.status = 'promoted' AND c.status = 'active'
                     AND (co.owner_user_id = ? OR w.id IS NOT NULL)
                   ORDER BY co.promoted_at DESC""",
                (user_id, user_id),
            ).fetchall()
            return [_row_to_content(r) for r in rows]

    # --- Unclaimed Content ---

    def get_unclaimed_content(self) -> list[Content]:
        """Get active content with no ownership record."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT c.* FROM content c
                   LEFT JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.id IS NULL AND c.status = 'active' AND c.size_bytes > 0
                   ORDER BY c.title""",
            ).fetchall()
            return [_row_to_content(r) for r in rows]

    def claim_content(self, content_id: int, user_id: int) -> ContentOwnership:
        """Create an ownership record for unclaimed content."""
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO content_ownership (content_id, owner_user_id, owned_at)
                   VALUES (?, ?, ?)""",
                (content_id, user_id, _now()),
            )
            row = conn.execute(
                "SELECT * FROM content_ownership WHERE content_id = ?", (content_id,)
            ).fetchone()
            return _row_to_ownership(row)

    # --- Admin Activity Feed ---

    def get_admin_activity_feed(self, limit: int = 50) -> list[dict]:
        """Get a chronological activity feed for admin Ship's Log."""
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM (
                    SELECT 'watch' as type, w.watched_at as at,
                           u.plex_username as actor, c.title, c.media_type,
                           owner_u.plex_username as owner_username,
                           NULL as size_bytes, NULL as viewers
                    FROM watch_events w
                    JOIN users u ON u.id = w.user_id
                    JOIN content c ON c.id = w.content_id
                    JOIN content_ownership co ON co.content_id = c.id
                    JOIN users owner_u ON owner_u.id = co.owner_user_id
                    WHERE w.completed = 1 AND w.user_id != co.owner_user_id

                    UNION ALL

                    SELECT 'promotion' as type, p.promoted_at as at,
                           u.plex_username as actor, c.title, c.media_type,
                           NULL, p.size_freed_bytes, p.unique_viewers
                    FROM promotion_log p
                    JOIN users u ON u.id = p.from_user_id
                    JOIN content c ON c.id = p.content_id

                    UNION ALL

                    SELECT 'deletion' as type, d.deleted_at as at,
                           u.plex_username as actor, d.title, NULL,
                           NULL, d.size_bytes, NULL
                    FROM deletion_log d
                    JOIN users u ON u.id = d.deleted_by_user_id
                ) ORDER BY at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [
                {
                    "type": row["type"],
                    "at": row["at"],
                    "actor": row["actor"],
                    "title": row["title"],
                    "media_type": row["media_type"],
                    "owner_username": row["owner_username"],
                    "size_bytes": row["size_bytes"],
                    "viewers": row["viewers"],
                }
                for row in rows
            ]


# --- Row mappers ---

def _row_to_user(row: sqlite3.Row) -> User:
    return User(
        id=row["id"],
        plex_user_id=row["plex_user_id"],
        plex_username=row["plex_username"],
        email=row["email"],
        quota_bytes=row["quota_bytes"],
        bonus_bytes=row["bonus_bytes"],
        is_admin=bool(row["is_admin"]),
        created_at=row["created_at"],
        auto_scuttle_days=row["auto_scuttle_days"] or 0,
        onboarded=bool(row["onboarded"]),
    )


def _row_to_content(row: sqlite3.Row) -> Content:
    keys = row.keys() if hasattr(row, "keys") else []
    return Content(
        id=row["id"],
        title=row["title"],
        media_type=row["media_type"],
        tmdb_id=row["tmdb_id"],
        sonarr_id=row["sonarr_id"],
        radarr_id=row["radarr_id"],
        overseerr_request_id=row["overseerr_request_id"],
        size_bytes=row["size_bytes"],
        status=row["status"],
        added_at=row["added_at"],
        poster_path=row["poster_path"] if "poster_path" in keys else None,
    )


def _row_to_ownership(row: sqlite3.Row) -> ContentOwnership:
    keys = row.keys() if hasattr(row, "keys") else []
    return ContentOwnership(
        id=row["id"],
        content_id=row["content_id"],
        owner_user_id=row["owner_user_id"],
        status=row["status"],
        owned_at=row["owned_at"],
        promoted_at=row["promoted_at"],
        plank_started_at=row["plank_started_at"] if "plank_started_at" in keys else None,
        buried_at=row["buried_at"] if "buried_at" in keys else None,
    )


def _row_to_promotion(row: sqlite3.Row) -> PromotionRecord:
    return PromotionRecord(
        id=row["id"],
        content_id=row["content_id"],
        from_user_id=row["from_user_id"],
        unique_viewers=row["unique_viewers"],
        size_freed_bytes=row["size_freed_bytes"],
        promoted_at=row["promoted_at"],
    )


def _row_to_deletion(row: sqlite3.Row) -> DeletionRecord:
    return DeletionRecord(
        id=row["id"],
        content_id=row["content_id"],
        deleted_by_user_id=row["deleted_by_user_id"],
        title=row["title"],
        size_bytes=row["size_bytes"],
        deleted_at=row["deleted_at"],
    )
