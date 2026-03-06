"""Tests for branding customisation."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from treasurr.app import _template_cache, create_app
from treasurr.config import Config, QuotaConfig, SafetyConfig


@pytest.fixture(autouse=True)
def clear_template_cache():
    """Clear template cache between tests."""
    _template_cache.clear()
    yield
    _template_cache.clear()


@pytest.fixture
def branding_dir(tmp_path):
    """Provide a temporary branding directory."""
    d = tmp_path / "branding"
    d.mkdir()
    return d


@pytest.fixture
def app_and_db(branding_dir):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    config = Config(
        db_path=path,
        quotas=QuotaConfig(
            promotion_mode="full_plunder",
            shared_plunder_max_bytes=0,
            min_retention_days=0,
            display_mode="exact",
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    from datetime import datetime, timedelta, timezone

    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    admin = db.upsert_user(
        plex_user_id="admin_1",
        plex_username="captain",
        email="cap@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )
    db.create_session("admin-token", admin.id, "plex-token-admin", expires)

    user = db.upsert_user(
        plex_user_id="user_1",
        plex_username="deckhand",
        email="deck@sea.com",
        quota_bytes=500_000_000_000,
    )
    db.create_session("user-token", user.id, "plex-token-user", expires)

    yield app, db, admin, user, path, branding_dir
    os.unlink(path)


@pytest.fixture
def admin_client(app_and_db):
    app, _, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-token")
    return client


@pytest.fixture
def user_client(app_and_db):
    app, _, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "user-token")
    return client


class TestBrandingSettings:
    def test_defaults_returned(self, admin_client):
        resp = admin_client.get("/api/admin/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["instance_name"] == "TREASURR"
        assert data["instance_tagline"] == "Your treasure. Your crew. Your plunder."
        assert data["custom_css"] == ""
        assert data["logo_filename"] == ""

    def test_update_instance_name(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"instance_name": "My Server"},
        )
        assert resp.status_code == 200
        assert resp.json()["instance_name"] == "My Server"

    def test_update_tagline(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"instance_tagline": "Welcome aboard!"},
        )
        assert resp.status_code == 200
        assert resp.json()["instance_tagline"] == "Welcome aboard!"

    def test_update_custom_css(self, admin_client):
        css = ":root { --gold: #ff0000; }"
        resp = admin_client.put(
            "/api/admin/settings",
            json={"custom_css": css},
        )
        assert resp.status_code == 200
        assert resp.json()["custom_css"] == css

    def test_instance_name_too_long(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"instance_name": "x" * 51},
        )
        assert resp.status_code == 400

    def test_tagline_too_long(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"instance_tagline": "x" * 101},
        )
        assert resp.status_code == 400

    def test_custom_css_too_long(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"custom_css": "x" * 10001},
        )
        assert resp.status_code == 400


class TestLogoUpload:
    def test_upload_valid_png(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.png", b"\x89PNG\r\n\x1a\n" + b"\x00" * 100, "image/png")},
            )
        assert resp.status_code == 200
        assert resp.json()["filename"] == "logo.png"
        assert (branding_dir / "logo.png").exists()

    def test_upload_valid_svg(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("icon.svg", b"<svg></svg>", "image/svg+xml")},
            )
        assert resp.status_code == 200
        assert resp.json()["filename"] == "logo.svg"

    def test_upload_replaces_existing(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.png", b"\x89PNG" + b"\x00" * 100, "image/png")},
            )
            resp = admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.jpg", b"\xff\xd8\xff" + b"\x00" * 100, "image/jpeg")},
            )
        assert resp.status_code == 200
        assert resp.json()["filename"] == "logo.jpg"
        assert not (branding_dir / "logo.png").exists()
        assert (branding_dir / "logo.jpg").exists()

    def test_reject_non_image(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("script.js", b"alert(1)", "application/javascript")},
            )
        assert resp.status_code == 400
        assert "Invalid file type" in resp.json()["detail"]

    def test_reject_oversized(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.png", b"\x89PNG" + b"\x00" * (513 * 1024), "image/png")},
            )
        assert resp.status_code == 400
        assert "512 KB" in resp.json()["detail"]

    def test_non_admin_cannot_upload(self, user_client, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = user_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.png", b"\x89PNG" + b"\x00" * 100, "image/png")},
            )
        assert resp.status_code == 403


class TestLogoDelete:
    def test_delete_existing_logo(self, admin_client, app_and_db, branding_dir):
        _, db, _, _, _, _ = app_and_db
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            admin_client.post(
                "/api/admin/branding/logo",
                files={"file": ("logo.png", b"\x89PNG" + b"\x00" * 100, "image/png")},
            )
            resp = admin_client.delete("/api/admin/branding/logo")
        assert resp.status_code == 200
        assert not (branding_dir / "logo.png").exists()
        assert db.get_setting("logo_filename", "") == ""

    def test_delete_when_no_logo(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.api.admin.BRANDING_DIR", branding_dir):
            resp = admin_client.delete("/api/admin/branding/logo")
        assert resp.status_code == 200


class TestTemplateRendering:
    def test_index_shows_default_branding(self, admin_client):
        resp = admin_client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert "TREASURR" in html
        assert "Your treasure. Your crew. Your plunder." in html
        assert "&#9875;" in html

    def test_index_shows_custom_name(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("instance_name", "MediaHub")
        resp = admin_client.get("/")
        assert resp.status_code == 200
        assert "MediaHub" in resp.text

    def test_index_shows_custom_tagline(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("instance_tagline", "Welcome aboard!")
        resp = admin_client.get("/")
        assert resp.status_code == 200
        assert "Welcome aboard!" in resp.text

    def test_index_shows_custom_css(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("custom_css", ":root { --gold: #ff0000; }")
        resp = admin_client.get("/")
        assert resp.status_code == 200
        assert "--gold: #ff0000" in resp.text

    def test_index_shows_logo_img(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("logo_filename", "logo.png")
        resp = admin_client.get("/")
        assert resp.status_code == 200
        assert '<img src="/branding/logo.png"' in resp.text

    def test_admin_shows_custom_name(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("instance_name", "MyPlex")
        resp = admin_client.get("/admin")
        assert resp.status_code == 200
        assert "MyPlex" in resp.text

    def test_admin_shows_custom_css(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("custom_css", ".header { background: red; }")
        resp = admin_client.get("/admin")
        assert resp.status_code == 200
        assert ".header { background: red; }" in resp.text

    def test_xss_escaped_in_name(self, admin_client, app_and_db):
        _, db, _, _, _, _ = app_and_db
        db.set_setting("instance_name", "<script>alert(1)</script>")
        resp = admin_client.get("/")
        assert resp.status_code == 200
        assert "<script>alert(1)</script>" not in resp.text
        assert "&lt;script&gt;" in resp.text


class TestBrandingFileServing:
    def test_serve_uploaded_logo(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.app.BRANDING_DIR", branding_dir):
            (branding_dir / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
            resp = admin_client.get("/branding/logo.png")
        assert resp.status_code == 200

    def test_404_for_missing_file(self, admin_client, app_and_db, branding_dir):
        with patch("treasurr.app.BRANDING_DIR", branding_dir):
            resp = admin_client.get("/branding/nonexistent.png")
        assert resp.status_code == 404
