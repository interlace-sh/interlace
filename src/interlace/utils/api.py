"""
API client for easy API integration in Interlace models.

Provides a simple, programmatic interface for making API calls with built-in
support for authentication, retry logic, pagination, rate limiting, and
automatic data conversion.
"""

import asyncio
import re
import time
from collections.abc import Callable
from typing import Any

import aiohttp
import ibis

from interlace.utils.logging import get_logger

logger = get_logger("interlace.utils.api")


class TokenBucket:
    """
    Token bucket rate limiter.

    Allows a maximum number of requests per time interval. Tokens are refilled
    at a constant rate. If no tokens are available, requests wait until a token
    becomes available.

    Example:
        bucket = TokenBucket(rate=10, interval=1.0)  # 10 requests per second
        await bucket.acquire()  # Consumes one token, waits if needed
    """

    def __init__(self, rate: float, interval: float = 1.0):
        """
        Initialize token bucket.

        Args:
            rate: Maximum number of requests allowed
            interval: Time interval in seconds (default: 1.0)
        """
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if interval <= 0:
            raise ValueError("interval must be > 0")
        self.rate = rate
        self.interval = interval
        self.tokens = float(rate)  # Start with full bucket
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()
        self.refill_rate = rate / interval  # Tokens per second

    async def acquire(self) -> None:
        """
        Acquire a token from the bucket.

        If no tokens are available, waits until a token is refilled.
        """
        while True:
            # print('acquire', self.tokens, self.last_refill)
            wait_time = 0.0
            async with self.lock:
                # Refill tokens based on elapsed time
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.rate, self.tokens + elapsed * self.refill_rate)
                self.last_refill = now

                # If we have at least one token, consume it and return
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # No tokens available, calculate wait time until next token is available
                # We need at least 1.0 token
                tokens_needed = 1.0 - self.tokens
                wait_time = tokens_needed / self.refill_rate

            # Wait outside the lock for tokens to refill
            # This allows other tasks to acquire the lock and check/refill tokens
            await asyncio.sleep(min(wait_time * 1.01, 0.1))


class API:
    """
    Helper class for making API calls in Interlace models.

    Designed to be simple and programmatic - pass in what you need, not config-driven.
    Supports authentication, retry logic, pagination, rate limiting, and automatic
    data conversion to formats compatible with Interlace.

    Example:
        ```python
        from interlace import model, API

        # Create shared API instance at module level for global rate limiting
        api = API(base_url="https://api.example.com", max_concurrent=10)

        @model(name="users", materialize="table")
        async def users():
            async with api:
                data = await api.get("/users")
                return data  # Returns ibis.Table, list, or dict

        @model(name="orders", materialize="table")
        async def orders():
            # Uses the SAME API instance - shares rate limiting with users()
            async with api:
                data = await api.get("/orders")
                return data
        ```
    """

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        auth: Callable[[aiohttp.ClientSession], Any] | None = None,
        max_concurrent: int = 10,
        max_retries: int = 5,
        retry_delay: float = 1.0,
        timeout: int = 120,
        convert_camel_case: bool = True,
        rate_limit: int | None = None,
        rate_limit_interval: float = 1.0,
    ):
        """
        Initialize API helper.

        Args:
            base_url: Base URL for API (e.g., "https://api.example.com")
            headers: Default headers to include in all requests
            auth: Optional async function that takes session and returns auth headers/token
                  Example: `async def auth(session): return {"Authorization": f"Bearer {token}"}`
            max_concurrent: Maximum concurrent requests (default: 10)
            max_retries: Maximum retry attempts for failed requests (default: 5)
            retry_delay: Delay between retries in seconds (default: 1.0)
            timeout: Request timeout in seconds (default: 120)
            convert_camel_case: Convert camelCase keys to snake_case (default: True)
            rate_limit: Maximum requests per interval (e.g., 10 = 10 requests per second)
                        If None, no rate limiting (default: None)
            rate_limit_interval: Time interval in seconds for rate limiting (default: 1.0)
        """
        self.base_url = base_url.rstrip("/")
        self.default_headers = headers or {}
        self.auth_func = auth
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.convert_camel_case = convert_camel_case

        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._session_lock = asyncio.Lock()
        self._session_refcount = 0  # Track how many contexts are using the session

        # Token bucket for rate limiting
        self.token_bucket: TokenBucket | None = None
        if rate_limit is not None:
            self.token_bucket = TokenBucket(rate=rate_limit, interval=rate_limit_interval)

    async def _ensure_session(self) -> None:
        """Ensure session exists, creating it if needed."""
        async with self._session_lock:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession(
                    base_url=self.base_url,
                    timeout=self.timeout,
                    headers=self.default_headers,
                )

                # Apply authentication if provided
                if self.auth_func:
                    try:
                        auth_result = await self.auth_func(self.session)
                        if isinstance(auth_result, dict):
                            self.session.headers.update(auth_result)
                        elif isinstance(auth_result, str):
                            # If auth returns a token string, assume Authorization header
                            self.session.headers["Authorization"] = f"Bearer {auth_result}"
                    except Exception as e:
                        logger.warning(f"Auth function failed: {e}, continuing without auth")

    async def __aenter__(self) -> "API":
        """Async context manager entry."""
        await self._ensure_session()
        async with self._session_lock:
            self._session_refcount += 1
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """Async context manager exit."""
        async with self._session_lock:
            self._session_refcount -= 1
            # Only close session if no other contexts are using it
            if self._session_refcount == 0 and self.session and not self.session.closed:
                await self.session.close()
                self.session = None

    async def close(self) -> None:
        """Explicitly close the session (useful for cleanup)."""
        async with self._session_lock:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            self._session_refcount = 0

    def _convert_camel_case(self, text: str) -> str:
        """Convert camelCase to snake_case."""
        # Insert underscore before uppercase letters (but not at start)
        text = re.sub(r"(?<!^)(?=[A-Z])", "_", text)
        return text.lower()

    def _convert_dict_keys_camel_case(self, data: list[dict] | dict) -> list[dict] | dict:
        """
        Convert camelCase keys to snake_case in dict(s).

        Args:
            data: Single dict or list of dicts

        Returns:
            Dict or list of dicts with converted keys
        """
        if not self.convert_camel_case:
            return data

        if isinstance(data, dict):
            # Single dict - convert keys
            return {self._convert_camel_case(k): v for k, v in data.items()}
        elif isinstance(data, list):
            # List of dicts - convert keys in each dict
            return [
                ({self._convert_camel_case(k): v for k, v in item.items()} if isinstance(item, dict) else item)
                for item in data
            ]

        return data

    def _to_ibis_memtable(self, data: list[dict] | dict) -> ibis.Table:
        """
        Convert JSON data to ibis memtable.

        Handles:
        - List of dicts -> memtable
        - Single dict -> memtable with one row
        - Converts camelCase to snake_case if enabled
        - Empty data -> empty memtable

        The executor will handle dict-to-list conversion automatically, but we
        do it here for camelCase conversion on keys.
        """
        # Convert camelCase keys if enabled
        data = self._convert_dict_keys_camel_case(data)

        # Handle empty data - return empty memtable with dummy schema
        # DuckDB doesn't support truly empty tables, but ibis.memtable([]) should work
        if not data or (isinstance(data, list) and len(data) == 0):
            # Return empty memtable - executor will handle empty case
            # Use a list with a dummy dict to create schema, then filter to empty
            # Actually, let's just return empty memtable and let executor handle it
            return ibis.memtable([])

        # Convert single dict to list (ibis.memtable accepts list of dicts directly)
        if isinstance(data, dict):
            data = [data]

        # ibis.memtable accepts list of dicts directly - no DataFrame needed!
        return ibis.memtable(data)

    def _extract_data(self, json_data: dict, data_attribute: str | None = "data") -> list[dict] | dict:
        """
        Extract data from JSON response.

        If data_attribute is specified and exists in the response, returns that value.
        Otherwise returns the entire response. The executor will handle dict-to-list
        conversion automatically if needed.

        Args:
            json_data: JSON response as dict
            data_attribute: Key to extract from response (default: "data")

        Returns:
            Extracted value (dict, list, or any) or entire response
        """
        if data_attribute and data_attribute in json_data:
            value = json_data[data_attribute]
            if value is not None:
                return value  # type: ignore[no-any-return]

        return json_data

    async def _fetch_with_retry(
        self,
        method: str,
        url: str,
        data: dict | None = None,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> dict:
        """
        Fetch data from API with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL path (relative to base_url) or full URL
            data: Request body (for POST/PUT)
            params: Query parameters
            headers: Additional headers for this request

        Returns:
            JSON response as dict

        Raises:
            aiohttp.ClientError: If request fails after retries
        """
        # Ensure session exists before making request
        await self._ensure_session()

        async with self.semaphore:
            # Apply token bucket rate limiting if enabled
            if self.token_bucket:
                await self.token_bucket.acquire()

            retries = 0
            last_error = None

            while retries < self.max_retries:
                try:
                    # Ensure session is still valid (might have been closed)
                    if self.session is None or self.session.closed:
                        await self._ensure_session()

                    assert self.session is not None
                    request_headers = {**self.session.headers}
                    if headers:
                        request_headers.update(headers)

                    # Convert boolean params to strings (aiohttp/yarl requirement)
                    if params:
                        clean_params = {}
                        for k, v in params.items():
                            if isinstance(v, bool):
                                clean_params[k] = str(v).lower()
                            else:
                                clean_params[k] = v
                        params = clean_params

                    # Handle both relative and absolute URLs
                    request_url = url if url.startswith("http") else url

                    start_time = time.monotonic()
                    async with self.session.request(
                        method,
                        request_url,
                        json=data,
                        params=params,
                        headers=request_headers,
                    ) as response:
                        end_time = time.monotonic()
                        duration = end_time - start_time
                        log_level = logger.debug if response.status <= 299 else logger.warning
                        log_level(
                            f"{method} {self.base_url}{request_url} {response.status} {duration:.2f}s {response.headers.get("Content-Length") or ""}"
                        )

                        if response.status <= 299:
                            return await response.json()  # type: ignore[no-any-return]
                        else:
                            text = await response.text()
                            logger.warning(f"{method} {response.status} {url} - {text[:200]}")
                            response.raise_for_status()

                except Exception as e:
                    last_error = e
                    retries += 1
                    if retries < self.max_retries:
                        logger.debug(f"Retry {retries}/{self.max_retries} for {url}")
                        await asyncio.sleep(self.retry_delay * retries)  # Exponential backoff
                    else:
                        logger.error(f"Failed after {self.max_retries} retries: {url}")
                        raise last_error from e

            # Should never reach here, but just in case
            raise last_error or Exception("Unknown error")

    async def request(
        self,
        url: str,
        method: str = "GET",
        data: dict | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        data_attribute: str | None = "data",
        dataframe: bool = True,
    ) -> ibis.Table | list[dict] | dict:
        """
        Make a single API request.

        Args:
            url: URL path (relative to base_url) or full URL
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            data: Request body (for POST/PUT)
            params: Query parameters
            headers: Additional headers for this request
            data_attribute: Key to extract from JSON response (default: "data")
                          Set to None to return full response
            dataframe: Return as ibis.Table (True) or raw dict/list (False)

        Returns:
            ibis.Table (memtable) if dataframe=True, otherwise dict or list
        """
        json_data = await self._fetch_with_retry(method, url, data, params, headers)
        extracted = self._extract_data(json_data, data_attribute)

        if dataframe:
            # Return ibis memtable directly (executor expects this)
            return self._to_ibis_memtable(extracted)
        else:
            return extracted

    async def get(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        data_attribute: str | None = "data",
        dataframe: bool = True,
    ) -> ibis.Table | list[dict] | dict:
        """Convenience method for GET requests."""
        return await self.request(
            url=url,
            method="GET",
            params=params,
            headers=headers,
            data_attribute=data_attribute,
            dataframe=dataframe,
        )

    async def post(
        self,
        url: str,
        data: dict | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        data_attribute: str | None = "data",
        dataframe: bool = True,
    ) -> ibis.Table | list[dict] | dict:
        """Convenience method for POST requests."""
        return await self.request(
            url=url,
            method="POST",
            data=data,
            params=params,
            headers=headers,
            data_attribute=data_attribute,
            dataframe=dataframe,
        )

    async def paginated(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        page_size_param: str = "pageSize",
        page_param: str = "page",
        count_attribute: str = "meta.count",
        data_attribute: str = "data",
        dataframe: bool = True,
        page_size: int = 100,
    ) -> ibis.Table | list[dict]:
        """
        Fetch paginated data automatically.

        Handles pagination by detecting total count and fetching all pages.
        Works with common pagination patterns:
        - Query params: ?page=1&pageSize=100
        - Response meta: {"meta": {"count": 500}, "data": [...]}

        Args:
            url: URL path (relative to base_url)
            params: Query parameters (will be updated with pagination params)
            headers: Additional headers
            page_size_param: Query param name for page size (default: "pageSize")
            page_param: Query param name for page number (default: "page")
            count_attribute: Dot-separated path to total count in response (default: "meta.count")
            data_attribute: Key to extract from JSON response (default: "data")
            dataframe: Return as ibis.Table (True) or list (False)
            page_size: Number of items per page (default: 100)

        Returns:
            ibis.Table (memtable) or list with all pages combined
        """
        # Start with first page
        first_params = {**(params or {}), page_size_param: page_size, page_param: 1}
        first_response = await self._fetch_with_retry("GET", url, params=first_params, headers=headers)

        # Extract first page data
        first_data = self._extract_data(first_response, data_attribute)
        all_data = first_data if isinstance(first_data, list) else [first_data]

        # Get total count
        count = self._get_nested_value(first_response, count_attribute)
        if count is None:
            # No pagination info, return first page only
            logger.warning(f"No pagination info found in response for {url}, returning first page only")
            if dataframe:
                return self._to_ibis_memtable(all_data)
            return all_data

        # Calculate number of pages
        total_pages = (count // page_size) + (1 if count % page_size > 0 else 0)

        if total_pages <= 1:
            # Only one page, return early
            if dataframe:
                return self._to_ibis_memtable(all_data)
            return all_data

        # Fetch remaining pages in parallel
        tasks = []
        for page in range(2, total_pages + 1):
            page_params = {**(params or {}), page_size_param: page_size, page_param: page}
            tasks.append(self._fetch_with_retry("GET", url, params=page_params, headers=headers))

        # Wait for all pages
        pages = await asyncio.gather(*tasks, return_exceptions=True)

        # Extract data from each page
        for page_response in pages:
            if isinstance(page_response, BaseException):
                logger.error(f"Error fetching page: {page_response}")
                continue
            page_data = self._extract_data(page_response, data_attribute)
            if isinstance(page_data, list):
                all_data.extend(page_data)
            else:
                all_data.append(page_data)

        if dataframe:
            return self._to_ibis_memtable(all_data)
        return all_data

    def _get_nested_value(self, data: dict, path: str) -> Any | None:
        """Get nested value from dict using dot-separated path."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    async def batch(
        self,
        urls: list[str],
        method: str = "GET",
        data: dict | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        data_attribute: str | None = "data",
        dataframe: bool = True,
    ) -> ibis.Table | list[dict]:
        """
        Make multiple API requests in parallel.

        Args:
            urls: List of URL paths (relative to base_url) or full URLs
            method: HTTP method (default: GET)
            data: Request body (same for all requests)
            params: Query parameters (same for all requests)
            headers: Additional headers (same for all requests)
            data_attribute: Key to extract from JSON response (default: "data")
            dataframe: Return as ibis.Table (True) or list (False)

        Returns:
            ibis.Table (memtable) or list with all results combined
        """
        tasks = [self._fetch_with_retry(method, url, data, params, headers) for url in urls]

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        all_data: list[dict] = []
        for response in responses:
            if isinstance(response, BaseException):
                logger.error(f"Error in batch request: {response}")
                continue
            extracted = self._extract_data(response, data_attribute)
            if isinstance(extracted, list):
                all_data.extend(extracted)
            else:
                all_data.append(extracted)

        if dataframe:
            return self._to_ibis_memtable(all_data)
        return all_data


# Convenience functions for common auth patterns


async def oauth2_token(
    session: aiohttp.ClientSession,
    token_url: str,
    client_id: str,
    client_secret: str,
    grant_type: str = "client_credentials",
    scope: str | None = None,
) -> str:
    """
    Get OAuth2 token using client credentials flow.

    Example:
        ```python
        async with API(
            base_url="https://api.example.com",
            auth=lambda session: oauth2_token(session, "https://auth.example.com/token", "id", "secret")
        ) as api:
            data = await api.get("/users")
        ```

    Args:
        session: aiohttp session
        token_url: OAuth2 token endpoint URL
        client_id: Client ID
        client_secret: Client secret
        grant_type: Grant type (default: "client_credentials")
        scope: Optional scope

    Returns:
        Access token string
    """
    data = {
        "grant_type": grant_type,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope

    async with session.post(token_url, data=data) as response:
        response.raise_for_status()
        token_data = await response.json()
        return str(token_data["access_token"])


async def basic_auth_token(
    session: aiohttp.ClientSession,
    auth_url: str,
    username: str,
    password: str,
) -> dict[str, str]:
    """
    Get auth token using basic auth (legacy pattern).

    Example:
        ```python
        async with API(
            base_url="https://api.example.com",
            auth=lambda session: basic_auth_token(session, "https://auth.example.com", "user", "pass")
        ) as api:
            data = await api.get("/users")
        ```

    Args:
        session: aiohttp session
        auth_url: Authentication endpoint URL
        username: Username
        password: Password

    Returns:
        Dict with auth headers (e.g., {"api-token": "..."})
    """
    async with session.post(auth_url, auth=aiohttp.BasicAuth(username, password)) as response:
        response.raise_for_status()
        token_data = await response.json()
        # Return as headers dict - adjust keys based on your API
        return {"api-token": token_data.get("token", token_data.get("access_token", ""))}
