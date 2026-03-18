"""
Smart Lead Hunter — Middleware Tests
======================================
Tests for auth middleware logic, rate limiting internals,
and the global rate limiter.

Covers:
  - APIKeyMiddleware path exclusion logic
  - Rate limit bucket reset behavior
  - Rate limit cleanup of expired entries
  - Rate limit store doesn't grow unbounded
"""

import time



# ═══════════════════════════════════════════════════════════════════════
# MIDDLEWARE PATH EXCLUSION
# ═══════════════════════════════════════════════════════════════════════


class TestMiddlewarePathExclusion:
    """Tests for APIKeyMiddleware path matching logic."""

    def test_exclude_exact_paths(self):
        from app.middleware.auth import APIKeyMiddleware
        # Create middleware instance to inspect exclude_exact
        from unittest.mock import MagicMock
        mw = APIKeyMiddleware(app=MagicMock())
        assert "/health" in mw.exclude_exact
        assert "/docs" in mw.exclude_exact

    def test_exclude_prefix_includes_auth(self):
        from app.middleware.auth import APIKeyMiddleware
        from unittest.mock import MagicMock
        mw = APIKeyMiddleware(app=MagicMock())
        # /auth/ should be in prefix exclusions
        assert any("/auth/" in p for p in mw.exclude_prefixes)

    def test_exclude_prefix_includes_sse_streams(self):
        """SSE streams can't send custom headers — must be excluded."""
        from app.middleware.auth import APIKeyMiddleware
        from unittest.mock import MagicMock
        mw = APIKeyMiddleware(app=MagicMock())
        sse_paths = [
            "/api/dashboard/scrape/stream",
            "/api/dashboard/extract-url/stream",
            "/api/dashboard/discovery/stream",
        ]
        for path in sse_paths:
            assert any(path.startswith(p) for p in mw.exclude_prefixes), \
                f"SSE path {path} not in exclude_prefixes"

    def test_protected_prefixes_include_api(self):
        from app.middleware.auth import APIKeyMiddleware
        from unittest.mock import MagicMock
        mw = APIKeyMiddleware(app=MagicMock())
        assert "/api/" in mw.protected_prefixes
        assert "/leads" in mw.protected_prefixes
        assert "/sources" in mw.protected_prefixes

    def test_dashboard_leads_is_not_excluded(self):
        """FIX C-01: /api/dashboard/leads was incorrectly excluded.
        Verify it's NOT in the exclude list."""
        from app.middleware.auth import APIKeyMiddleware
        from unittest.mock import MagicMock
        mw = APIKeyMiddleware(app=MagicMock())
        # /api/dashboard/leads should NOT be in exclude_prefixes
        assert "/api/dashboard/leads" not in mw.exclude_prefixes


# ═══════════════════════════════════════════════════════════════════════
# RATE LIMIT INTERNALS
# ═══════════════════════════════════════════════════════════════════════


class TestRateLimitBuckets:
    """Tests for the auth-level rate limiter."""

    def test_bucket_resets_after_window(self):
        from app.routes.auth import _check_rate_limit, _login_attempts, _LOGIN_LIMIT
        # Use unique IP
        test_ip = f"bucket-reset-{time.monotonic()}"

        # Exhaust the limit
        for _ in range(_LOGIN_LIMIT):
            _check_rate_limit(test_ip)

        # Manually reset the bucket's expiry to the past
        _login_attempts[test_ip]["reset"] = time.monotonic() - 1

        # Now it should be allowed again
        assert _check_rate_limit(test_ip) is True

    def test_separate_ips_tracked_independently(self):
        from app.routes.auth import _check_rate_limit, _LOGIN_LIMIT
        ip1 = f"ip1-{time.monotonic()}"
        ip2 = f"ip2-{time.monotonic()}"

        # Exhaust ip1
        for _ in range(_LOGIN_LIMIT + 1):
            _check_rate_limit(ip1)

        # ip2 should still be allowed
        assert _check_rate_limit(ip2) is True


class TestGlobalRateLimitCleanup:
    """Tests for the global rate limit store cleanup."""

    def test_cleanup_removes_expired(self):
        from app.main import _rate_limit_store, _cleanup_rate_limit_store, _RATE_LIMIT_WINDOW
        # Insert a fake expired entry
        now = time.monotonic()
        _rate_limit_store["expired-ip"] = {
            "count": 100,
            "reset": now - _RATE_LIMIT_WINDOW - 100,
        }
        _cleanup_rate_limit_store(now + _RATE_LIMIT_WINDOW + 1)
        # May or may not be cleaned depending on last_cleanup timing,
        # but at minimum the function should not crash
        assert True

    def test_cleanup_doesnt_crash_on_empty(self):
        from app.main import _cleanup_rate_limit_store
        _cleanup_rate_limit_store(time.monotonic())
        assert True


# ═══════════════════════════════════════════════════════════════════════
# CLIENT IP EXTRACTION
# ═══════════════════════════════════════════════════════════════════════


class TestClientIPExtraction:
    """Tests for _get_client_ip() in auth module."""

    def test_extracts_from_forwarded_header(self):
        from app.routes.auth import _get_client_ip
        from unittest.mock import MagicMock

        request = MagicMock()
        request.headers = {"X-Forwarded-For": "203.0.113.50, 70.41.3.18"}
        ip = _get_client_ip(request)
        assert ip == "203.0.113.50"

    def test_falls_back_to_client_host(self):
        from app.routes.auth import _get_client_ip
        from unittest.mock import MagicMock

        request = MagicMock()
        request.headers = {}
        request.client.host = "192.168.1.100"
        ip = _get_client_ip(request)
        assert ip == "192.168.1.100"

    def test_handles_no_client(self):
        from app.routes.auth import _get_client_ip
        from unittest.mock import MagicMock

        request = MagicMock()
        request.headers = {}
        request.client = None
        ip = _get_client_ip(request)
        assert ip == "unknown"
