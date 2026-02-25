"""
SMART LEAD HUNTER - AI RATE LIMITER
===================================
Intelligent rate limiting for AI API calls (Gemini + Ollama).

PROBLEM:
- Gemini has rate limits (429 Too Many Requests)
- Constantly hitting Gemini when rate limited wastes time
- Need smart fallback to Ollama when Gemini is unavailable

SOLUTION:
- Track rate limit state per provider
- Automatic cooldown when rate limited
- Smart provider selection based on availability
- Minimum delay between requests

Usage:
    from app.services.ai_rate_limiter import AIRateLimiter

    limiter = AIRateLimiter()

    # Before each AI call
    provider = await limiter.get_available_provider()

    # After a successful call
    limiter.record_success(provider)

    # After a rate limit error
    limiter.record_rate_limit(provider)
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class AIProvider(Enum):
    """Available AI providers"""

    GEMINI = "gemini"
    OLLAMA = "ollama"


@dataclass
class ProviderState:
    """State tracking for an AI provider"""

    name: str
    is_available: bool = True
    cooldown_until: float = 0.0  # Unix timestamp
    last_request: float = 0.0  # Unix timestamp
    consecutive_errors: int = 0
    total_requests: int = 0
    total_successes: int = 0
    total_rate_limits: int = 0

    def is_ready(self) -> bool:
        """Check if provider is ready for a request"""
        now = time.time()
        return self.is_available and now >= self.cooldown_until


class AIRateLimiter:
    """
    Manages rate limiting for AI providers.

    Features:
    - Tracks cooldown state per provider
    - Automatic provider switching when rate limited
    - Minimum delay between requests
    - Exponential backoff on repeated rate limits
    """

    # Configuration
    MIN_DELAY_GEMINI = 0.2
    MIN_DELAY_OLLAMA = 0.5  # Minimum seconds between Ollama requests (local, fast)
    INITIAL_COOLDOWN = 15.0  # Initial cooldown after rate limit (seconds)
    MAX_COOLDOWN = 120.0  # Maximum cooldown (5 minutes)
    COOLDOWN_MULTIPLIER = 1.5  # Multiply cooldown on repeated rate limits
    RECOVERY_THRESHOLD = 3  # Successes needed to reset cooldown

    def __init__(self):
        """Initialize rate limiter with provider states"""
        self._providers = {
            AIProvider.GEMINI: ProviderState(name="Gemini"),
            AIProvider.OLLAMA: ProviderState(name="Ollama"),
        }
        self._current_cooldown = {
            AIProvider.GEMINI: self.INITIAL_COOLDOWN,
            AIProvider.OLLAMA: self.INITIAL_COOLDOWN,
        }
        self._lock = asyncio.Lock()
        self._gemini_disabled_until = 0.0  # Extended disable for Gemini

    async def get_available_provider(
        self, preferred: AIProvider = AIProvider.GEMINI
    ) -> Optional[AIProvider]:
        """
        Get the best available provider for a request.

        Args:
            preferred: Preferred provider if available

        Returns:
            Available provider, or None if all are rate limited
        """
        async with self._lock:
            now = time.time()

            # Check if Gemini is in extended cooldown
            if now < self._gemini_disabled_until:
                remaining = int(self._gemini_disabled_until - now)
                if remaining % 30 == 0:  # Log every 30 seconds
                    logger.info(
                        f"⏳ Gemini cooldown: {remaining}s remaining, using Ollama"
                    )
                return (
                    AIProvider.OLLAMA
                    if self._providers[AIProvider.OLLAMA].is_ready()
                    else None
                )

            # Try preferred provider first
            if self._providers[preferred].is_ready():
                return preferred

            # Try other provider
            other = (
                AIProvider.OLLAMA
                if preferred == AIProvider.GEMINI
                else AIProvider.GEMINI
            )
            if self._providers[other].is_ready():
                return other

            # Both in cooldown - check which has shorter wait
            gemini_wait = max(
                0, self._providers[AIProvider.GEMINI].cooldown_until - now
            )
            ollama_wait = max(
                0, self._providers[AIProvider.OLLAMA].cooldown_until - now
            )

        # M3 FIX: Sleep OUTSIDE the lock so other providers aren't blocked
        if gemini_wait <= ollama_wait and gemini_wait > 0:
            logger.info(f"⏳ Waiting {gemini_wait:.1f}s for Gemini cooldown...")
            await asyncio.sleep(gemini_wait)
            return AIProvider.GEMINI
        elif ollama_wait > 0:
            logger.info(f"⏳ Waiting {ollama_wait:.1f}s for Ollama cooldown...")
            await asyncio.sleep(ollama_wait)
            return AIProvider.OLLAMA

        return AIProvider.OLLAMA  # Default fallback

    async def wait_before_request(self, provider: AIProvider) -> None:
        """
        Wait the minimum required time before making a request.

        M3 FIX: Calculate wait inside lock, sleep outside, then re-acquire
        to update last_request timestamp.

        Args:
            provider: Provider to wait for
        """
        # Calculate wait time inside lock
        async with self._lock:
            now = time.time()
            state = self._providers[provider]
            min_delay = (
                self.MIN_DELAY_GEMINI
                if provider == AIProvider.GEMINI
                else self.MIN_DELAY_OLLAMA
            )

            wait_time = 0.0
            if state.last_request > 0:
                elapsed = now - state.last_request
                if elapsed < min_delay:
                    wait_time = min_delay - elapsed

        # Sleep OUTSIDE the lock
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        # Re-acquire lock to update timestamp
        async with self._lock:
            self._providers[provider].last_request = time.time()
            self._providers[provider].total_requests += 1

    def record_success(self, provider: AIProvider) -> None:
        """
        Record a successful API call.

        Args:
            provider: Provider that succeeded
        """
        state = self._providers[provider]
        state.total_successes += 1
        state.consecutive_errors = 0

        # Reset cooldown multiplier after successes
        if state.total_successes % self.RECOVERY_THRESHOLD == 0:
            self._current_cooldown[provider] = self.INITIAL_COOLDOWN

    def record_rate_limit(self, provider: AIProvider) -> None:
        """
        Record a rate limit error and set cooldown.

        Args:
            provider: Provider that was rate limited
        """
        state = self._providers[provider]
        state.total_rate_limits += 1
        state.consecutive_errors += 1

        # Calculate cooldown with exponential backoff
        cooldown = self._current_cooldown[provider]
        if state.consecutive_errors > 1:
            cooldown = min(cooldown * self.COOLDOWN_MULTIPLIER, self.MAX_COOLDOWN)

        self._current_cooldown[provider] = cooldown
        state.cooldown_until = time.time() + cooldown

        logger.warning(
            f"⚠️ {state.name} rate limited! "
            f"Cooldown: {cooldown:.0f}s "
            f"(consecutive errors: {state.consecutive_errors})"
        )

        # If Gemini has too many consecutive errors, disable for longer
        if provider == AIProvider.GEMINI and state.consecutive_errors >= 3:
            extended_cooldown = 45.0
            self._gemini_disabled_until = time.time() + extended_cooldown
            logger.warning(
                f"🚫 Gemini disabled for {extended_cooldown:.0f}s due to repeated rate limits. "
                f"Using Ollama exclusively."
            )

    def record_error(self, provider: AIProvider, error: str) -> None:
        """
        Record a non-rate-limit error.

        Args:
            provider: Provider that errored
            error: Error message
        """
        state = self._providers[provider]
        state.consecutive_errors += 1

        # Short cooldown for errors (not as long as rate limits)
        if state.consecutive_errors >= 3:
            state.cooldown_until = time.time() + 30.0
            logger.warning(
                f"⚠️ {state.name} errored {state.consecutive_errors}x, cooling down 30s"
            )

    def get_stats(self) -> dict:
        """Get statistics for all providers"""
        return {
            provider.value: {
                "total_requests": state.total_requests,
                "total_successes": state.total_successes,
                "total_rate_limits": state.total_rate_limits,
                "success_rate": (
                    state.total_successes / max(1, state.total_requests) * 100
                ),
                "is_available": state.is_ready(),
                "consecutive_errors": state.consecutive_errors,
            }
            for provider, state in self._providers.items()
        }

    def reset(self) -> None:
        """Reset all provider states"""
        for provider in self._providers.values():
            provider.is_available = True
            provider.cooldown_until = 0.0
            provider.consecutive_errors = 0
        self._gemini_disabled_until = 0.0


# Singleton instance
_rate_limiter: Optional[AIRateLimiter] = None


def get_rate_limiter() -> AIRateLimiter:
    """Get the global rate limiter instance"""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = AIRateLimiter()
    return _rate_limiter
