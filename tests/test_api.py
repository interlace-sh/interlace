"""
Comprehensive tests for the API helper class.

Tests cover:
- Initialization and configuration
- Authentication (OAuth2, basic auth, custom)
- HTTP methods (GET, POST, request)
- Retry logic
- Rate limiting
- Pagination
- Batch requests
- Data conversion (camelCase to snake_case)
- Error handling
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any
import ibis
import aiohttp
from aioresponses import aioresponses
from interlace.utils.api import API, oauth2_token, basic_auth_token


class TestAPIInitialization:
    """Test API initialization and configuration."""
    
    def test_api_init_with_defaults(self):
        """Test API initializes with default configuration values."""
        api = API(base_url="https://api.example.com")
        
        assert api.base_url == "https://api.example.com"
        assert api.default_headers == {}
        assert api.auth_func is None
        assert api.max_concurrent == 10
        assert api.max_retries == 5
        assert api.retry_delay == 1.0
        assert api.timeout.total == 120
        assert api.convert_camel_case is True
        assert api.session is None
    
    def test_api_init_with_custom_config(self):
        """Test API uses custom configuration values."""
        api = API(
            base_url="https://api.example.com",
            headers={"X-Custom": "value"},
            max_concurrent=5,
            max_retries=3,
            retry_delay=2.0,
            timeout=60,
            convert_camel_case=False,
        )
        
        assert api.base_url == "https://api.example.com"
        assert api.default_headers == {"X-Custom": "value"}
        assert api.max_concurrent == 5
        assert api.max_retries == 3
        assert api.retry_delay == 2.0
        assert api.timeout.total == 60
        assert api.convert_camel_case is False
    
    def test_api_init_strips_trailing_slash(self):
        """Test that base_url trailing slash is stripped."""
        api = API(base_url="https://api.example.com/")
        assert api.base_url == "https://api.example.com"


class TestAPIContextManager:
    """Test async context manager functionality."""
    
    @pytest.mark.asyncio
    async def test_context_manager_creates_session(self):
        """Test that context manager creates and closes session."""
        api = API(base_url="https://api.example.com")
        
        async with api:
            assert api.session is not None
            assert isinstance(api.session, aiohttp.ClientSession)
            # Check private attribute _base_url
            assert str(api.session._base_url) == "https://api.example.com"
        
        # Session should be closed after context exit (set to None with ref counting)
        assert api.session is None or api.session.closed
    
    @pytest.mark.asyncio
    async def test_context_manager_sets_default_headers(self):
        """Test that default headers are set on session."""
        api = API(
            base_url="https://api.example.com",
            headers={"X-Custom": "value", "Authorization": "Bearer token"}
        )
        
        async with api:
            assert api.session.headers["X-Custom"] == "value"
            assert api.session.headers["Authorization"] == "Bearer token"
    
    @pytest.mark.asyncio
    async def test_context_manager_applies_auth_dict(self):
        """Test that auth function returning dict updates headers."""
        async def auth_func(session):
            return {"Authorization": "Bearer token123"}
        
        api = API(base_url="https://api.example.com", auth=auth_func)
        
        async with api:
            assert api.session.headers["Authorization"] == "Bearer token123"
    
    @pytest.mark.asyncio
    async def test_context_manager_applies_auth_string(self):
        """Test that auth function returning string sets Authorization header."""
        async def auth_func(session):
            return "token123"
        
        api = API(base_url="https://api.example.com", auth=auth_func)
        
        async with api:
            assert api.session.headers["Authorization"] == "Bearer token123"
    
    @pytest.mark.asyncio
    async def test_context_manager_handles_auth_failure(self):
        """Test that auth failure is handled gracefully."""
        async def auth_func(session):
            raise Exception("Auth failed")
        
        api = API(base_url="https://api.example.com", auth=auth_func)
        
        # Should not raise, but log warning
        async with api:
            assert api.session is not None
            # Auth header should not be set
            assert "Authorization" not in api.session.headers or api.session.headers.get("Authorization") != "Bearer token123"


class TestAPIGet:
    """Test GET request functionality."""
    
    @pytest.mark.asyncio
    async def test_get_simple_request(self):
        """Test simple GET request."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1, "name": "Alice"}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
                # Use ibis syntax to access row data
                row =  result.limit(1).execute()
                assert row.id.item() == 1
                assert row.name.item() == "Alice"
    
    @pytest.mark.asyncio
    async def test_get_with_params(self):
        """Test GET request with query parameters."""
        with aioresponses() as m:
            m.get("https://api.example.com/users?limit=10", payload={"data": [{"id": 1}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", params={"limit": 10})
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_get_with_custom_headers(self):
        """Test GET request with custom headers."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": []})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", headers={"X-Custom": "value"})
                
                assert isinstance(result, ibis.Table)
    
    @pytest.mark.asyncio
    async def test_get_returns_raw_data(self):
        """Test GET request returning raw dict/list instead of table."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1, "name": "Alice"}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", dataframe=False)
                
                assert isinstance(result, list)
                assert len(result) == 1  # list has len() support
                assert result[0]["id"] == 1
    
    @pytest.mark.asyncio
    async def test_get_custom_data_attribute(self):
        """Test GET request with custom data attribute."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"results": [{"id": 1}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", data_attribute="results")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_get_no_data_attribute(self):
        """Test GET request with no data attribute (returns full response)."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"id": 1, "name": "Alice"})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", data_attribute=None)
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
                # Use ibis syntax to access row data
                assert result.limit(1).id.execute().item() == 1


class TestAPIPost:
    """Test POST request functionality."""
    
    @pytest.mark.asyncio
    async def test_post_with_data(self):
        """Test POST request with JSON data."""
        with aioresponses() as m:
            m.post("https://api.example.com/users", payload={"data": {"id": 1, "name": "Alice"}})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.post("/users", data={"name": "Alice"})
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_post_with_params(self):
        """Test POST request with query parameters."""
        with aioresponses() as m:
            m.post("https://api.example.com/users?validate=true", payload={"data": []})
            
            async with API(base_url="https://api.example.com") as api:
                # API helper now converts boolean to string automatically
                result = await api.post("/users", data={}, params={"validate": True})
                
                assert isinstance(result, ibis.Table)


class TestAPIRetry:
    """Test retry logic."""
    
    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test that requests are retried on failure."""
        with aioresponses() as m:
            # First two calls fail, third succeeds
            m.get("https://api.example.com/users", exception=aiohttp.ClientConnectionError("Connection error"))
            m.get("https://api.example.com/users", exception=aiohttp.ClientConnectionError("Connection error"))
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            
            api = API(base_url="https://api.example.com", max_retries=3, retry_delay=0.01)
            
            async with api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_error(self):
        """Test that error is raised when all retries are exhausted."""
        with aioresponses() as m:
            # All calls fail
            for _ in range(5):
                m.get("https://api.example.com/users", exception=aiohttp.ClientConnectionError("Connection error"))
            
            api = API(base_url="https://api.example.com", max_retries=5, retry_delay=0.01)
            
            async with api:
                with pytest.raises(aiohttp.ClientConnectionError):
                    await api.get("/users")
    
    @pytest.mark.asyncio
    async def test_no_retry_on_success(self):
        """Test that successful requests are not retried."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Verify only one request was made
                assert len(m.requests) == 1


class TestAPIRateLimiting:
    """Test rate limiting with semaphore."""
    
    @pytest.mark.asyncio
    async def test_rate_limiting_respects_max_concurrent(self):
        """Test that rate limiting respects max_concurrent setting."""
        # Patch semaphore acquire to add delay to simulate slow API
        original_acquire = asyncio.Semaphore.acquire
        
        async def delayed_acquire(self):
            """Add 1 second delay after acquiring semaphore to simulate slow API."""
            result = await original_acquire(self)
            await asyncio.sleep(0.5)
            return result
        
        with aioresponses() as m:
            # Mock 20 requests with payload
            for i in range(20):
                m.get(
                    f"https://api.example.com/endpoint{i}",
                    payload={"data": []},
                    repeat=True  # Allow multiple calls for retries
                )
            
            api = API(base_url="https://api.example.com", max_concurrent=5)
            
            # Patch Semaphore.acquire to add delay after acquisition
            with patch.object(asyncio.Semaphore, 'acquire', delayed_acquire):
                async with api:
                    start_time = time.time()
                    
                    # Make 20 concurrent requests
                    # With max_concurrent=5, should take ~4 seconds (20 requests / 5 concurrent = 4 batches)
                    tasks = [api.get(f"/endpoint{i}") for i in range(20)]
                    results = await asyncio.gather(*tasks)
                    
                    elapsed = time.time() - start_time
                    
                    # All should succeed
                    assert len(results) == 20
                    # With 5 concurrent and 0.5s delay per request:
                    # 20 requests / 5 concurrent = 4 batches
                    # Each batch takes ~0.5s, so total should be ~2s
                    # Allow some margin for overhead (1.5s to 2.5s)
                    assert 1.5 <= elapsed <= 2.5, f"Expected ~2s (1.5-2.5s range), got {elapsed:.2f}s"


class TestAPIPagination:
    """Test pagination functionality."""
    
    @pytest.mark.asyncio
    async def test_paginated_single_page(self):
        """Test paginated request with only one page."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/users?pageSize=100&page=1",
                payload={"meta": {"count": 50}, "data": [{"id": i} for i in range(50)]}
            )
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.paginated("/users", page_size=100)
                
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 50
    
    @pytest.mark.asyncio
    async def test_paginated_multiple_pages(self):
        """Test paginated request with multiple pages."""
        with aioresponses() as m:
            # First page
            m.get(
                "https://api.example.com/users?pageSize=100&page=1",
                payload={"meta": {"count": 250}, "data": [{"id": i} for i in range(100)]}
            )
            # Remaining pages (page 2 has 100 items, page 3 has 50 items)
            m.get(
                "https://api.example.com/users?pageSize=100&page=2",
                payload={"meta": {"count": 250}, "data": [{"id": i} for i in range(100, 200)]}
            )
            m.get(
                "https://api.example.com/users?pageSize=100&page=3",
                payload={"meta": {"count": 250}, "data": [{"id": i} for i in range(200, 250)]}
            )
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.paginated("/users", page_size=100)
                
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 250
    
    @pytest.mark.asyncio
    async def test_paginated_no_pagination_info(self):
        """Test paginated request when no pagination info is available."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/users?pageSize=100&page=1",
                payload={"data": [{"id": 1}]}  # No meta.count
            )
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.paginated("/users", page_size=100)
                
                # Should return first page only with warning
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_paginated_custom_count_attribute(self):
        """Test paginated request with custom count attribute."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/users?pageSize=100&page=1",
                payload={"pagination": {"total": 150}, "data": [{"id": i} for i in range(100)]}
            )
            m.get(
                "https://api.example.com/users?pageSize=100&page=2",
                payload={"pagination": {"total": 150}, "data": [{"id": i} for i in range(100, 150)]}
            )
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.paginated(
                    "/users",
                    page_size=100,
                    count_attribute="pagination.total"
                )
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 150


class TestAPIBatch:
    """Test batch request functionality."""
    
    @pytest.mark.asyncio
    async def test_batch_multiple_urls(self):
        """Test batch request with multiple URLs."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            m.get("https://api.example.com/products", payload={"data": [{"id": 2}]})
            m.get("https://api.example.com/orders", payload={"data": [{"id": 3}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.batch(["/users", "/products", "/orders"])
                
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 3
    
    @pytest.mark.asyncio
    async def test_batch_handles_errors(self):
        """Test that batch request handles individual errors gracefully."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            m.get("https://api.example.com/products", exception=aiohttp.ClientError("Error"))
            m.get("https://api.example.com/orders", payload={"data": [{"id": 3}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.batch(["/users", "/products", "/orders"])
                
                # Should return data from successful requests
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 2  # users + orders, products failed


class TestAPIDataConversion:
    """Test data conversion functionality."""
    
    @pytest.mark.asyncio
    async def test_camel_case_conversion(self):
        """Test that camelCase keys are converted to snake_case."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/users",
                payload={"data": [{"userId": 1, "userName": "Alice", "createdAt": "2024-01-01"}]}
            )
            
            async with API(base_url="https://api.example.com", convert_camel_case=True) as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Check columns in schema (schema() returns dict-like with column names as keys)
                column_names = list(result.schema().keys())
                assert "user_id" in column_names
                assert "user_name" in column_names
                assert "created_at" in column_names
    
    @pytest.mark.asyncio
    async def test_no_camel_case_conversion(self):
        """Test that camelCase conversion can be disabled."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/users",
                payload={"data": [{"userId": 1, "userName": "Alice"}]}
            )
            
            async with API(base_url="https://api.example.com", convert_camel_case=False) as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Check columns in schema (schema() returns dict-like with column names as keys)
                column_names = list(result.schema().keys())
                assert "userId" in column_names
                assert "userName" in column_names
    
    @pytest.mark.asyncio
    async def test_single_dict_to_table(self):
        """Test that single dict is converted to table with one row."""
        with aioresponses() as m:
            m.get("https://api.example.com/user/1", payload={"data": {"id": 1, "name": "Alice"}})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/user/1")
                
                assert isinstance(result, ibis.Table)
                # Use ibis syntax to check data
                count = result.count().execute()
                assert count == 1
                assert result.limit(1).id.execute().item() == 1
    
    @pytest.mark.asyncio
    async def test_empty_list_to_dataframe(self):
        """Test that empty list returns empty memtable."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": []})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Empty memtables can't be executed with DuckDB (requires at least one column)
                # Just verify it's an ibis.Table - the empty memtable is valid
                # In practice, empty responses would be handled at the model level


class TestAPIDataExtraction:
    """Test data extraction from JSON responses."""
    
    @pytest.mark.asyncio
    async def test_extract_data_attribute(self):
        """Test extraction of data from 'data' attribute."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}], "meta": {}})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", data_attribute="data")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_extract_custom_attribute(self):
        """Test extraction of data from custom attribute."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"results": [{"id": 1}], "meta": {}})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", data_attribute="results")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_extract_list_response(self):
        """Test extraction when response is already a list."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload=[{"id": 1}, {"id": 2}])
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/users", data_attribute=None)
                
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 2
    
    @pytest.mark.asyncio
    async def test_extract_dict_response(self):
        """Test extraction when response is a dict."""
        with aioresponses() as m:
            m.get("https://api.example.com/user/1", payload={"id": 1, "name": "Alice"})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("/user/1", data_attribute=None)
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1


class TestAPIAuthHelpers:
    """Test authentication helper functions."""
    
    @pytest.mark.asyncio
    async def test_oauth2_token(self):
        """Test OAuth2 token helper."""
        with aioresponses() as m:
            m.post(
                "https://auth.example.com/token",
                payload={"access_token": "token123", "token_type": "Bearer", "expires_in": 3600}
            )
            
            session = aiohttp.ClientSession()
            try:
                token = await oauth2_token(
                    session,
                    token_url="https://auth.example.com/token",
                    client_id="client_id",
                    client_secret="client_secret"
                )
                
                assert token == "token123"
            finally:
                await session.close()
    
    @pytest.mark.asyncio
    async def test_oauth2_token_with_scope(self):
        """Test OAuth2 token helper with scope."""
        with aioresponses() as m:
            m.post(
                "https://auth.example.com/token",
                payload={"access_token": "token123", "token_type": "Bearer"}
            )
            
            session = aiohttp.ClientSession()
            try:
                token = await oauth2_token(
                    session,
                    token_url="https://auth.example.com/token",
                    client_id="client_id",
                    client_secret="client_secret",
                    scope="read write"
                )
                
                assert token == "token123"
            finally:
                await session.close()
    
    @pytest.mark.asyncio
    async def test_basic_auth_token(self):
        """Test basic auth token helper."""
        with aioresponses() as m:
            m.post(
                "https://auth.example.com/auth",
                payload={"token": "api_token_123"}
            )
            
            session = aiohttp.ClientSession()
            try:
                headers = await basic_auth_token(
                    session,
                    auth_url="https://auth.example.com/auth",
                    username="user",
                    password="pass"
                )
                
                assert isinstance(headers, dict)
                assert "api-token" in headers
                assert headers["api-token"] == "api_token_123"
            finally:
                await session.close()
    
    @pytest.mark.asyncio
    async def test_basic_auth_token_with_access_token(self):
        """Test basic auth token helper when response has access_token."""
        with aioresponses() as m:
            m.post(
                "https://auth.example.com/auth",
                payload={"access_token": "token123"}
            )
            
            session = aiohttp.ClientSession()
            try:
                headers = await basic_auth_token(
                    session,
                    auth_url="https://auth.example.com/auth",
                    username="user",
                    password="pass"
                )
                
                assert headers["api-token"] == "token123"
            finally:
                await session.close()


class TestAPIErrorHandling:
    """Test error handling."""
    
    @pytest.mark.asyncio
    async def test_http_error_raises(self):
        """Test that HTTP errors are raised."""
        with aioresponses() as m:
            # Use pattern matching to handle base_url
            m.get("https://api.example.com/users", status=404, payload={"error": "Not found"}, repeat=True)
            
            async with API(base_url="https://api.example.com") as api:
                # The API helper will call raise_for_status() which raises ClientResponseError
                with pytest.raises(aiohttp.ClientResponseError):
                    await api.get("/users")
    
    @pytest.mark.asyncio
    async def test_non_200_status_raises(self):
        """Test that non-2xx status codes raise errors."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", status=500, payload={"error": "Server error"}, repeat=True)
            
            async with API(base_url="https://api.example.com") as api:
                with pytest.raises(aiohttp.ClientResponseError):
                    await api.get("/users")
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self):
        """Test that timeouts are handled."""
        # Note: Testing actual timeouts with aioresponses is complex
        # This test verifies that timeout configuration is set correctly
        api = API(base_url="https://api.example.com", timeout=1)
        
        async with api:
            assert api.timeout.total == 1
            # Actual timeout behavior is tested through integration tests


class TestAPIRequestMethod:
    """Test generic request method."""
    
    @pytest.mark.asyncio
    async def test_request_get(self):
        """Test generic request method with GET."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.request("/users", method="GET")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_request_put(self):
        """Test generic request method with PUT."""
        with aioresponses() as m:
            m.put("https://api.example.com/users/1", payload={"data": {"id": 1}})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.request("/users/1", method="PUT", data={"name": "Alice"})
                
                assert isinstance(result, ibis.Table)
    
    @pytest.mark.asyncio
    async def test_request_delete(self):
        """Test generic request method with DELETE."""
        with aioresponses() as m:
            m.delete("https://api.example.com/users/1", payload={"success": True})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.request("/users/1", method="DELETE", dataframe=False)
                
                assert isinstance(result, dict)
                assert result["success"] is True


class TestAPITokenBucket:
    """Test token bucket rate limiting."""
    
    @pytest.mark.asyncio
    async def test_token_bucket_rate_limiting(self):
        """Test that token bucket enforces rate limits."""
        import time
        
        with aioresponses() as m:
            # Mock 20 requests
            for i in range(20):
                m.get(f"https://api.example.com/endpoint{i}", payload={"data": []})
            
            # Create API with rate limit of 10 requests per second
            api = API(base_url="https://api.example.com", rate_limit=10, rate_limit_interval=1.0)
            
            async with api:
                start_time = time.time()
                
                # Make 20 requests - should take at least 1 second due to rate limiting
                tasks = [api.get(f"/endpoint{i}") for i in range(20)]
                results = await asyncio.gather(*tasks)
                
                elapsed = time.time() - start_time
                
                # All should succeed
                assert len(results) == 20
                # Should take at least ~1 second (10 requests per second = 2 seconds for 20 requests)
                # Allow some margin for test execution overhead
                assert elapsed >= 0.95, f"Expected at least 0.95s, got {elapsed}s"
    
    @pytest.mark.asyncio
    async def test_token_bucket_without_rate_limit(self):
        """Test that API works without rate limiting."""
        with aioresponses() as m:
            # Mock 10 requests
            for i in range(10):
                m.get(f"https://api.example.com/endpoint{i}", payload={"data": []})
            
            # Create API without rate limit
            api = API(base_url="https://api.example.com", rate_limit=None)
            
            async with api:
                # Make 10 requests - should complete quickly
                tasks = [api.get(f"/endpoint{i}") for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                # All should succeed
                assert len(results) == 10


class TestAPIRealWorldScenarios:
    """Test real-world usage scenarios."""
    
    @pytest.mark.asyncio
    async def test_incremental_loading_pattern(self):
        """Test incremental loading pattern."""
        with aioresponses() as m:
            # Mock the paginated request with date filter
            # Note: aioresponses matches on URL including query params, so we need to match the exact URL
            m.get(
                "https://api.example.com/notifications?createdFrom=2024-01-01&page=1&pageSize=100",
                payload={"meta": {"count": 50}, "data": [{"id": i, "created_at": f"2024-01-{i:02d}"} for i in range(1, 51)]}
            )
            
            async with API(base_url="https://api.example.com") as api:
                params = {"pageSize": 100}
                # Simulate getting last date from database
                params["createdFrom"] = "2024-01-01"
                
                result = await api.paginated("/notifications", params=params, page_size=100)
                
                assert isinstance(result, ibis.Table)
                count = result.count().execute()
                assert count == 50
    
    @pytest.mark.asyncio
    async def test_multiple_endpoints_pattern(self):
        """Test pattern of fetching from multiple endpoints."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            m.get("https://api.example.com/products", payload={"data": [{"id": 2}]})
            m.get("https://api.example.com/orders", payload={"data": [{"id": 3}]})
            
            async with API(base_url="https://api.example.com") as api:
                users = await api.get("/users")
                products = await api.get("/products")
                orders = await api.get("/orders")
                
                # Use count() to get row count for ibis.Table
                assert users.count().execute() == 1
                assert products.count().execute() == 1
                assert orders.count().execute() == 1
    
    @pytest.mark.asyncio
    async def test_oauth2_integration(self):
        """Test full OAuth2 integration flow."""
        with aioresponses() as m:
            # OAuth2 token request
            m.post(
                "https://auth.example.com/token",
                payload={"access_token": "token123", "token_type": "Bearer"}
            )
            # API request with token
            m.get(
                "https://api.example.com/users",
                payload={"data": [{"id": 1}]}
            )
            
            async with API(
                base_url="https://api.example.com",
                auth=lambda session: oauth2_token(
                    session,
                    token_url="https://auth.example.com/token",
                    client_id="client_id",
                    client_secret="client_secret"
                )
            ) as api:
                result = await api.get("/users")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
                # Verify auth header was set
                assert "Authorization" in api.session.headers


class TestAPIEdgeCases:
    """Test edge cases and special scenarios."""
    
    @pytest.mark.asyncio
    async def test_absolute_url(self):
        """Test that absolute URLs work."""
        with aioresponses() as m:
            m.get("https://other-api.example.com/data", payload={"data": [{"id": 1}]})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.get("https://other-api.example.com/data")
                
                assert isinstance(result, ibis.Table)
                # Use count() to get row count
                count = result.count().execute()
                assert count == 1
    
    @pytest.mark.asyncio
    async def test_nested_data_extraction(self):
        """Test extraction of nested data structures."""
        with aioresponses() as m:
            m.get(
                "https://api.example.com/data",
                payload={"response": {"items": {"data": [{"id": 1}]}}}
            )
            
            async with API(base_url="https://api.example.com") as api:
                # Note: current implementation doesn't support nested paths
                # This test documents current behavior
                result = await api.get("/data", data_attribute="response")
                
                assert isinstance(result, ibis.Table)
    
    @pytest.mark.asyncio
    async def test_mixed_response_types(self):
        """Test handling of mixed response types in batch."""
        with aioresponses() as m:
            m.get("https://api.example.com/users", payload={"data": [{"id": 1}]})
            m.get("https://api.example.com/config", payload={"setting": "value"})
            
            async with API(base_url="https://api.example.com") as api:
                result = await api.batch(["/users", "/config"])
                
                assert isinstance(result, ibis.Table)
                # Should handle both list and dict responses

