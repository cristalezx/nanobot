"""Dynamic token fetcher — fetches a fresh auth token before each LLM call.

Usage (via ProviderConfig):
    token_url: "https://auth.example.com/token"
    token_method: "POST"
    token_body: {"client_id": "xxx", "client_secret": "yyy"}
    token_json_path: "access_token"        # or "data.token" for nested
    token_header_name: "Authorization"
    token_header_prefix: "Bearer "
    token_ttl: 3600                         # seconds; 0 = no cache
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import httpx
from loguru import logger

if TYPE_CHECKING:
    from nanobot.config.schema import ProviderConfig


class TokenFetcher:
    """Fetches and caches an auth token from a remote URL.

    Thread-safe via asyncio.Lock. One instance should be shared per provider.
    """

    def __init__(self, config: "ProviderConfig") -> None:
        self._url = config.token_url
        self._method = (config.token_method or "POST").upper()
        self._body: dict[str, Any] = dict(config.token_body) if config.token_body else {}
        self._json_path = config.token_json_path or "token"
        self._header_name = config.token_header_name or "Authorization"
        self._header_prefix = config.token_header_prefix  # may be ""
        self._ttl = config.token_ttl  # 0 means no caching

        self._cached_token: str | None = None
        self._expiry: float = 0.0
        self._lock = asyncio.Lock()

    async def get_header(self) -> dict[str, str]:
        """Return ``{header_name: "<prefix><token>"}`` with a valid token.

        Fetches a new token when the cache is empty or expired.
        """
        token = await self._get_token()
        return {self._header_name: f"{self._header_prefix}{token}"}

    async def _get_token(self) -> str:
        async with self._lock:
            now = time.monotonic()
            if self._cached_token and (self._ttl == 0 or now < self._expiry):
                return self._cached_token
            token = await self._fetch()
            self._cached_token = token
            self._expiry = now + self._ttl if self._ttl > 0 else 0.0
            return token

    async def _fetch(self) -> str:
        logger.debug("TokenFetcher: fetching token from {}", self._url)
        async with httpx.AsyncClient(timeout=10) as client:
            if self._method == "GET":
                resp = await client.get(self._url, params=self._body or None)
            else:
                resp = await client.post(self._url, json=self._body or None)
            resp.raise_for_status()
            data = resp.json()

        token = self._extract(data, self._json_path)
        if not isinstance(token, str) or not token:
            raise ValueError(
                f"TokenFetcher: expected a non-empty string at '{self._json_path}', "
                f"got {token!r} from {self._url}"
            )
        logger.debug("TokenFetcher: token refreshed (ttl={}s)", self._ttl)
        return token

    @staticmethod
    def _extract(data: Any, path: str) -> Any:
        """Walk a dot-separated path through nested dicts/lists."""
        for key in path.split("."):
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list):
                try:
                    data = data[int(key)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return data
