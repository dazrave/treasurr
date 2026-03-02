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
    QuotaSummary,
    QuotaTransaction,
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
    UNIQUE(tmdb_id, media_type)
);

CREATE TABLE IF NOT EXISTS content_ownership (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL UNIQUE,
    owner_user_id INTEGER NOT NULL,
    status TEXT DEFAULT 'owned' CHECK(status IN ('owned', 'promoted', 'released')),
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

    def update_content_status(self, content_id: int, status: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE content SET status = ? WHERE id = ?", (status, content_id))

    def update_content_arr_ids(self, content_id: int, sonarr_id: int | None = None, radarr_id: int | None = None) -> None:
        with self.connection() as conn:
            if sonarr_id is not None:
                conn.execute("UPDATE content SET sonarr_id = ? WHERE id = ?", (sonarr_id, content_id))
            if radarr_id is not None:
                conn.execute("UPDATE content SET radarr_id = ? WHERE id = ?", (radarr_id, content_id))

    def get_all_active_content(self) -> list[Content]:
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM content WHERE status = 'active' ORDER BY title").fetchall()
            return [_row_to_content(r) for r in rows]

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
                          co.owned_at, co.promoted_at,
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
                   WHERE co.status = 'owned' AND c.status = 'active'""",
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

    def get_quota_summary(self, user_id: int) -> QuotaSummary | None:
        with self.connection() as conn:
            user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            if not user_row:
                return None

            usage_row = conn.execute(
                """SELECT COALESCE(SUM(c.size_bytes), 0) as used_bytes,
                          COUNT(c.id) as owned_count
                   FROM content c
                   JOIN content_ownership co ON co.content_id = c.id
                   WHERE co.owner_user_id = ? AND co.status = 'owned' AND c.status = 'active'""",
                (user_id,),
            ).fetchone()

            return QuotaSummary(
                user_id=user_id,
                quota_bytes=user_row["quota_bytes"],
                bonus_bytes=user_row["bonus_bytes"],
                used_bytes=usage_row["used_bytes"],
                owned_count=usage_row["owned_count"],
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
                   WHERE co.status = 'owned' AND c.status = 'active'"""
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

            user_count = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            content_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM content WHERE status = 'active'"
            ).fetchone()["cnt"]

            return {
                "total_bytes": total,
                "owned_bytes": owned,
                "promoted_bytes": promoted,
                "unowned_bytes": unowned,
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
    )


def _row_to_content(row: sqlite3.Row) -> Content:
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
    )


def _row_to_ownership(row: sqlite3.Row) -> ContentOwnership:
    return ContentOwnership(
        id=row["id"],
        content_id=row["content_id"],
        owner_user_id=row["owner_user_id"],
        status=row["status"],
        owned_at=row["owned_at"],
        promoted_at=row["promoted_at"],
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
