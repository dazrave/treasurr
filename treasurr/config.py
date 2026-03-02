"""YAML configuration loader with environment variable overlay."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ApiConfig:
    url: str
    key: str
    type: str = "overseerr"


@dataclass(frozen=True)
class QuotaTier:
    name: str
    bytes: int


@dataclass(frozen=True)
class QuotaConfig:
    default_bytes: int = 536_870_912_000  # 500 GB
    promotion_threshold: int = 2
    promotion_exclude_requester: bool = True
    watch_completion_percent: int = 80
    tiers: tuple[QuotaTier, ...] = ()


@dataclass(frozen=True)
class SafetyConfig:
    max_deletions_per_hour: int = 10


@dataclass(frozen=True)
class Config:
    db_path: str = "/app/data/treasurr.db"
    host: str = "0.0.0.0"
    port: int = 8080
    sync_interval_seconds: int = 900
    timezone: str = "Europe/London"
    secret_key: str = ""
    plex_client_id: str = ""
    tautulli: ApiConfig = field(default_factory=lambda: ApiConfig(url="", key=""))
    overseerr: ApiConfig = field(default_factory=lambda: ApiConfig(url="", key=""))
    sonarr: ApiConfig = field(default_factory=lambda: ApiConfig(url="", key=""))
    radarr: ApiConfig = field(default_factory=lambda: ApiConfig(url="", key=""))
    quotas: QuotaConfig = field(default_factory=QuotaConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)


def load_config(path: str | Path = "config.yaml") -> Config:
    """Load config from YAML file, overlaying environment variables for secrets."""
    raw: dict = {}
    config_path = Path(path)
    if config_path.exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    general = raw.get("general", {})
    apis = raw.get("apis", {})
    quotas_raw = raw.get("quotas", {})
    safety_raw = raw.get("safety", {})

    tiers = tuple(
        QuotaTier(name=t["name"], bytes=t["bytes"])
        for t in quotas_raw.get("tiers", [])
    )

    return Config(
        db_path=general.get("db_path", "/app/data/treasurr.db"),
        host=general.get("host", "0.0.0.0"),
        port=general.get("port", 8080),
        sync_interval_seconds=general.get("sync_interval_seconds", 900),
        timezone=general.get("timezone", "Europe/London"),
        secret_key=os.environ.get("TREASURR_SECRET_KEY", "dev-secret-change-me"),
        plex_client_id=os.environ.get("TREASURR_PLEX_CLIENT_ID", ""),
        tautulli=ApiConfig(
            url=apis.get("tautulli", {}).get("url", ""),
            key=os.environ.get("TREASURR_TAUTULLI_KEY", ""),
        ),
        overseerr=ApiConfig(
            url=apis.get("overseerr", {}).get("url", ""),
            key=os.environ.get("TREASURR_OVERSEERR_KEY", ""),
            type=apis.get("overseerr", {}).get("type", "overseerr"),
        ),
        sonarr=ApiConfig(
            url=apis.get("sonarr", {}).get("url", ""),
            key=os.environ.get("TREASURR_SONARR_KEY", ""),
        ),
        radarr=ApiConfig(
            url=apis.get("radarr", {}).get("url", ""),
            key=os.environ.get("TREASURR_RADARR_KEY", ""),
        ),
        quotas=QuotaConfig(
            default_bytes=quotas_raw.get("default_bytes", 536_870_912_000),
            promotion_threshold=quotas_raw.get("promotion_threshold", 2),
            promotion_exclude_requester=quotas_raw.get("promotion_exclude_requester", True),
            watch_completion_percent=quotas_raw.get("watch_completion_percent", 80),
            tiers=tiers,
        ),
        safety=SafetyConfig(
            max_deletions_per_hour=safety_raw.get("max_deletions_per_hour", 10),
        ),
    )
