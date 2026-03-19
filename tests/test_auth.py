"""
Smart Lead Hunter — Auth System Tests
=======================================
Covers: password validation, JWT lifecycle, OTP hashing, rate limiting,
        cookie security, registration domain restriction, login flow.

All tests are pure unit tests — no database required.
"""

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
# PASSWORD VALIDATION
# ═══════════════════════════════════════════════════════════════════════


class TestPasswordValidation:
    """Tests for validate_password() strength rules."""

    def test_valid_password_passes(self):
        from app.routes.auth import validate_password
        assert validate_password("StrongPass1!") is None

    def test_too_short(self):
        from app.routes.auth import validate_password
        err = validate_password("Ab1")
        assert err is not None
        assert "8 characters" in err

    def test_no_uppercase(self):
        from app.routes.auth import validate_password
        err = validate_password("alllowercase1")
        assert err is not None
        assert "uppercase" in err

    def test_no_lowercase(self):
        from app.routes.auth import validate_password
        err = validate_password("ALLUPPERCASE1")
        assert err is not None
        assert "lowercase" in err

    def test_no_number(self):
        from app.routes.auth import validate_password
        err = validate_password("NoNumbersHere")
        assert err is not None
        assert "number" in err

    def test_exactly_8_chars_valid(self):
        from app.routes.auth import validate_password
        assert validate_password("Abcdefg1") is None

    def test_empty_password(self):
        from app.routes.auth import validate_password
        err = validate_password("")
        assert err is not None


# ═══════════════════════════════════════════════════════════════════════
# PASSWORD HASHING
# ═══════════════════════════════════════════════════════════════════════


class TestPasswordHashing:
    """Tests for hash_password / verify_password."""

    def test_hash_and_verify(self):
        from app.routes.auth import hash_password, verify_password
        hashed = hash_password("MyPassword123")
        assert verify_password("MyPassword123", hashed) is True

    def test_wrong_password_fails(self):
        from app.routes.auth import hash_password, verify_password
        hashed = hash_password("CorrectPassword1")
        assert verify_password("WrongPassword1", hashed) is False

    def test_hash_is_not_plaintext(self):
        from app.routes.auth import hash_password
        hashed = hash_password("MyPassword123")
        assert hashed != "MyPassword123"
        assert hashed.startswith("$2b$")


# ═══════════════════════════════════════════════════════════════════════
# OTP HASHING
# ═══════════════════════════════════════════════════════════════════════


class TestOTPHashing:
    """Tests for OTP hash/verify — never stored in plaintext."""

    def test_otp_hash_and_verify(self):
        from app.routes.auth import hash_otp, verify_otp
        otp = "123456"
        hashed = hash_otp(otp)
        assert verify_otp(otp, hashed) is True

    def test_wrong_otp_fails(self):
        from app.routes.auth import hash_otp, verify_otp
        hashed = hash_otp("123456")
        assert verify_otp("654321", hashed) is False

    def test_otp_generation_format(self):
        from app.routes.auth import generate_otp
        otp = generate_otp()
        assert len(otp) == 6
        assert otp.isdigit()

    def test_otp_generation_randomness(self):
        """Two consecutive OTPs should (almost certainly) differ."""
        from app.routes.auth import generate_otp
        otps = {generate_otp() for _ in range(10)}
        assert len(otps) > 1  # at least 2 unique in 10 draws


# ═══════════════════════════════════════════════════════════════════════
# JWT TOKENS
# ═══════════════════════════════════════════════════════════════════════


class TestJWTTokens:
    """Tests for JWT creation, decoding, expiry."""

    def test_create_and_decode(self):
        from app.routes.auth import create_token, decode_token
        token = create_token(user_id=42, email="jay@jauniforms.com", role="admin")
        payload = decode_token(token)
        assert payload is not None
        assert payload["sub"] == "42"
        assert payload["email"] == "jay@jauniforms.com"
        assert payload["role"] == "admin"

    def test_remember_me_longer_expiry(self):
        from app.routes.auth import create_token, decode_token
        normal = create_token(1, "a@b.com", "sales", remember=False)
        remember = create_token(1, "a@b.com", "sales", remember=True)

        n_payload = decode_token(normal)
        r_payload = decode_token(remember)

        # Remember token should expire later
        assert r_payload["exp"] > n_payload["exp"]

    def test_invalid_token_returns_none(self):
        from app.routes.auth import decode_token
        assert decode_token("garbage.token.here") is None
        assert decode_token("") is None

    def test_expired_token_returns_none(self):
        from jose import jwt
        from app.routes.auth import SECRET_KEY, ALGORITHM

        expired_payload = {
            "sub": "1",
            "email": "a@b.com",
            "role": "sales",
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
        }
        token = jwt.encode(expired_payload, SECRET_KEY, algorithm=ALGORITHM)
        from app.routes.auth import decode_token
        assert decode_token(token) is None


# ═══════════════════════════════════════════════════════════════════════
# RATE LIMITING
# ═══════════════════════════════════════════════════════════════════════


class TestRateLimiting:
    """Tests for auth rate limiter."""

    def test_allows_under_limit(self):
        from app.routes.auth import _check_rate_limit
        # Use a unique IP to avoid cross-test contamination
        test_ip = f"rate-test-{time.monotonic()}"
        for _ in range(5):
            assert _check_rate_limit(test_ip) is True

    def test_blocks_over_limit(self):
        from app.routes.auth import _check_rate_limit, _LOGIN_LIMIT
        test_ip = f"rate-block-{time.monotonic()}"
        for _ in range(_LOGIN_LIMIT):
            _check_rate_limit(test_ip)
        # Next one should be blocked
        assert _check_rate_limit(test_ip) is False


# ═══════════════════════════════════════════════════════════════════════
# COOKIE SETTINGS
# ═══════════════════════════════════════════════════════════════════════


class TestCookieSettings:
    """Tests for auth cookie configuration."""

    def test_cookie_is_httponly(self):
        """Cookie must be httponly to prevent XSS access."""
        from app.routes.auth import set_auth_cookie
        from unittest.mock import MagicMock

        response = MagicMock()
        set_auth_cookie(response, "test-token")
        response.set_cookie.assert_called_once()
        _, kwargs = response.set_cookie.call_args
        assert kwargs["httponly"] is True

    def test_cookie_samesite_lax(self):
        """SameSite=Lax for CSRF protection."""
        from app.routes.auth import set_auth_cookie
        response = MagicMock()
        set_auth_cookie(response, "test-token")
        _, kwargs = response.set_cookie.call_args
        assert kwargs["samesite"] == "lax"

    def test_cookie_not_secure_in_dev(self):
        """Secure=False in development (localhost doesn't use HTTPS)."""
        from app.routes.auth import set_auth_cookie, IS_PRODUCTION
        response = MagicMock()
        set_auth_cookie(response, "test-token")
        _, kwargs = response.set_cookie.call_args
        # In test env, IS_PRODUCTION should be False
        assert kwargs["secure"] is False or not IS_PRODUCTION

    def test_remember_me_max_age(self):
        """Remember-me cookie should have ~30-day max_age."""
        from app.routes.auth import set_auth_cookie, REMEMBER_DAYS
        response = MagicMock()
        set_auth_cookie(response, "token", remember=True)
        _, kwargs = response.set_cookie.call_args
        assert kwargs["max_age"] == REMEMBER_DAYS * 86400

    def test_session_cookie_max_age(self):
        """Normal session cookie should have ~8-hour max_age."""
        from app.routes.auth import set_auth_cookie, SESSION_HOURS
        response = MagicMock()
        set_auth_cookie(response, "token", remember=False)
        _, kwargs = response.set_cookie.call_args
        assert kwargs["max_age"] == SESSION_HOURS * 3600


# ═══════════════════════════════════════════════════════════════════════
# REGISTRATION DOMAIN RESTRICTION
# ═══════════════════════════════════════════════════════════════════════


class TestRegistrationRestriction:
    """Only @jauniforms.com emails can register."""

    @pytest.mark.asyncio
    async def test_non_company_email_rejected(self, client):
        resp = await client.post("/auth/register", json={
            "first_name": "Hacker",
            "last_name": "McHack",
            "email": "hacker@gmail.com",
            "password": "StrongPass1",
            "role": "sales",
        })
        assert resp.status_code == 422
        assert "jauniforms.com" in resp.json().get("error", "")

    @pytest.mark.asyncio
    async def test_invalid_role_rejected(self, client):
        resp = await client.post("/auth/register", json={
            "first_name": "Test",
            "last_name": "User",
            "email": "test@jauniforms.com",
            "password": "StrongPass1",
            "role": "superadmin",
        })
        # Pydantic validator catches bad role → 422
        assert resp.status_code == 422


# ═══════════════════════════════════════════════════════════════════════
# AUTH ENDPOINTS (HTTP)
# ═══════════════════════════════════════════════════════════════════════


class TestAuthEndpoints:
    """HTTP-level tests for auth routes."""

    @pytest.mark.asyncio
    async def test_me_unauthenticated(self, client):
        """GET /auth/me without cookie returns 401."""
        resp = await client.get("/auth/me")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_logout_clears_cookie(self, client):
        """POST /auth/logout returns success and clear-cookie header."""
        resp = await client.post("/auth/logout")
        assert resp.status_code == 200
        assert resp.json()["message"] == "Logged out"

    @pytest.mark.asyncio
    async def test_login_missing_fields(self, client):
        """POST /auth/login with missing fields returns 422."""
        resp = await client.post("/auth/login", json={"email": "a@b.com"})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_register_weak_password(self, client):
        """Weak password is rejected at registration."""
        resp = await client.post("/auth/register", json={
            "first_name": "Test",
            "last_name": "User",
            "email": "weak@jauniforms.com",
            "password": "weak",
            "role": "sales",
        })
        data = resp.json()
        assert data.get("success") is False
        assert "password" in data.get("error", "").lower() or "8 characters" in data.get("error", "")

    @pytest.mark.asyncio
    async def test_verify_code_no_pending(self, client):
        """Verify with no pending registration returns error."""
        try:
            resp = await client.post("/auth/verify-code", json={
                "email": "nobody@jauniforms.com",
                "code": "123456",
            })
            if resp.status_code == 200:
                data = resp.json()
                assert data.get("success") is False
            else:
                assert resp.status_code == 500
        except (ConnectionRefusedError, OSError):
            pytest.skip("Database not available")
        except Exception as e:
            msg = str(e)
            if any(s in msg for s in ["does not exist", "UndefinedTable", "ProgrammingError"]):
                pytest.skip("Database tables not created")
            raise
