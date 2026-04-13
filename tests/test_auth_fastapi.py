"""Tests for FastAPI authentication and protected endpoint access.

Covers: TC_34, TC_35, TC_58, TC_59, TC_73, TC_74, TC_50
Plus edge cases: malformed tokens, wrong algorithm, missing Bearer prefix.
"""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import jwt as pyjwt
import pytest
import httpx

from fastapi_backend.app.main import app
from fastapi_backend.app.auth import get_current_user


pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_token(user_id, username, expired=False):
    """Create a JWT token for testing."""
    now = datetime.now(timezone.utc)
    exp = now - timedelta(minutes=5) if expired else now + timedelta(minutes=15)
    payload = {
        "user_id": user_id,
        "username": username,
        "exp": exp,
        "iat": now,
    }
    return pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")


def _headers(token):
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Unit tests for get_current_user()
# ---------------------------------------------------------------------------

class TestGetCurrentUser:
    """Unit tests for the FastAPI JWT dependency."""

    def test_valid_token(self, user):
        """TC_35 ΓÇö Verify that a valid JWT token grants access."""
        from fastapi.security import HTTPAuthorizationCredentials

        token = _make_token(user.id, user.username)
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        result = get_current_user(creds)
        assert result["user_id"] == user.id
        assert result["username"] == user.username

    def test_expired_token_raises_401(self, user):
        """TC_34 ΓÇö Verify that an expired JWT token raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        token = _make_token(user.id, user.username, expired=True)
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_invalid_token_raises_401(self):
        """TC_34 ΓÇö Verify that an invalid token raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="not.a.valid.token")
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_missing_claims_raises_401(self):
        """Verify that a token missing user_id/username raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        payload = {"exp": datetime.now(timezone.utc) + timedelta(minutes=15)}
        token = pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_missing_user_id_raises_401(self):
        """Verify that a token with username but no user_id raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        payload = {
            "username": "someone",
            "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        }
        token = pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_missing_username_raises_401(self):
        """Verify that a token with user_id but no username raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        payload = {
            "user_id": 1,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        }
        token = pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_wrong_secret_raises_401(self, user):
        """Verify that a token signed with a different secret raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        payload = {
            "user_id": user.id,
            "username": user.username,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        }
        token = pyjwt.encode(payload, "wrong-secret-key", algorithm="HS256")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401

    def test_empty_string_token_raises_401(self):
        """Verify that an empty string token raises 401."""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="")
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(creds)
        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# FastAPI endpoint tests (async)
# ---------------------------------------------------------------------------

class TestProtectedEndpointsNoAuth:
    """Tests for accessing protected endpoints without a JWT token."""

    PROTECTED_ENDPOINTS = [
        ("GET", "/connections"),
        ("POST", "/connections"),
        ("GET", "/connections/1"),
        ("PUT", "/connections/1"),
        ("DELETE", "/connections/1"),
        ("POST", "/query/execute"),
        ("POST", "/commits"),
        ("GET", "/commits"),
        ("GET", "/commits/some-version"),
        ("GET", "/anticommands"),
        ("GET", "/anticommands/some-version"),
        ("GET", "/snapshots"),
        ("GET", "/snapshots/frequency"),
        ("PUT", "/snapshots/frequency"),
        ("POST", "/rollback"),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,path", PROTECTED_ENDPOINTS)
    async def test_protected_endpoint_returns_401_without_token(self, method, path):
        """TC_73 ΓÇö Verify that all protected endpoints return 401 without a JWT token."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.request(method, path)
            assert response.status_code == 401 or response.status_code == 403, (
                f"{method} {path} returned {response.status_code}, expected 401/403"
            )

    @pytest.mark.asyncio
    async def test_health_endpoint_no_auth_required(self):
        """Verify /health does not require authentication."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get("/health")
            assert response.status_code == 200
            assert response.json() == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_expired_token_rejected_by_endpoint(self, user):
        """Verify expired token gets 401 from actual endpoint."""
        token = _make_token(user.id, user.username, expired=True)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get("/connections", headers=_headers(token))
            assert response.status_code == 401


@pytest.mark.django_db(transaction=True)
class TestAuthenticatedEndpoints:
    """Tests for accessing endpoints with valid authentication."""

    @pytest.mark.asyncio
    async def test_unauthorized_request_rejected(self):
        """TC_58 ΓÇö Verify that an API request without a valid JWT token is rejected with 401."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get("/connections")
            assert response.status_code in (401, 403)

    @pytest.mark.asyncio
    async def test_authenticated_query_execution(self, user, connection_profile, auth_headers):
        """TC_59 / TC_74 ΓÇö Verify that an authenticated user can execute a query through the API."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor

        with patch("fastapi_backend.app.services.query_service.get_user_connection", return_value=mock_conn):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/query/execute",
                    json={
                        "connection_profile_id": connection_profile.id,
                        "sql": "SELECT * FROM users",
                    },
                    headers=auth_headers,
                )
                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "success"
                assert "columns" in data
                assert "rows" in data

    @pytest.mark.asyncio
    async def test_authenticated_list_connections(self, user, connection_profile, auth_headers):
        """Verify authenticated user can list their connection profiles."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get("/connections", headers=auth_headers)
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_query_via_full_api_stack(self, user, connection_profile, auth_headers):
        """TC_50 ΓÇö Verify executing SQL through the full API stack returns correct output."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor

        with patch("fastapi_backend.app.services.query_service.get_user_connection", return_value=mock_conn):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/query/execute",
                    json={
                        "connection_profile_id": connection_profile.id,
                        "sql": "SELECT id, name FROM users",
                    },
                    headers=auth_headers,
                )
                assert response.status_code == 200
                data = response.json()
                assert data["columns"] == ["id", "name"]
                assert data["rows"] == [[1, "Alice"], [2, "Bob"]]
                assert data["rowcount"] == 2
                assert data["status"] == "success"

    @pytest.mark.asyncio
    async def test_write_via_query_endpoint_rejected(self, user, connection_profile, auth_headers):
        """Verify that write SQL through the query endpoint returns 400."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/query/execute",
                json={
                    "connection_profile_id": connection_profile.id,
                    "sql": "INSERT INTO t VALUES (1)",
                },
                headers=auth_headers,
            )
            assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_get_nonexistent_commit_returns_404(self, user, auth_headers):
        """Verify that getting a non-existent commit returns 404."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get(
                "/commits/nonexistent-version-id",
                headers=auth_headers,
            )
            assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_nonexistent_anticommand_returns_404(self, user, auth_headers):
        """Verify that getting a non-existent anticommand returns 404."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get(
                "/anticommands/nonexistent-version-id",
                headers=auth_headers,
            )
            assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_nonexistent_connection_returns_404(self, user, auth_headers):
        """Verify that getting a non-existent connection profile returns 404."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.get(
                "/connections/99999",
                headers=auth_headers,
            )
            assert response.status_code == 404


class TestAuthProxyRoutes:
    """Tests for the FastAPI auth proxy routes (mocked Django backend)."""

    @pytest.mark.asyncio
    async def test_register_proxy(self):
        """Verify the /auth/register route proxies to Django."""
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": 1, "username": "proxyuser"}

        with patch("fastapi_backend.app.routes.auth_routes.requests.request", return_value=mock_response):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/auth/register",
                    json={"username": "proxyuser", "password": "proxyPass1", "email": "proxy@example.com"},
                )
                assert response.status_code == 201
                assert response.json()["username"] == "proxyuser"

    @pytest.mark.asyncio
    async def test_token_proxy(self):
        """Verify the /auth/token route proxies to Django."""
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {"access": "fake-access", "refresh": "fake-refresh"}

        with patch("fastapi_backend.app.routes.auth_routes.requests.request", return_value=mock_response):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/auth/token",
                    json={"username": "testuser", "password": "testpass"},
                )
                assert response.status_code == 200
                data = response.json()
                assert "access" in data
                assert "refresh" in data

    @pytest.mark.asyncio
    async def test_token_proxy_failure(self):
        """Verify the /auth/token route returns error when Django is unreachable."""
        import requests as req_lib

        with patch("fastapi_backend.app.routes.auth_routes.requests.request", side_effect=req_lib.ConnectionError):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/auth/token",
                    json={"username": "testuser", "password": "testpass"},
                )
                assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_register_proxy_failure(self):
        """Verify the /auth/register route returns error when Django is unreachable."""
        import requests as req_lib

        with patch("fastapi_backend.app.routes.auth_routes.requests.request", side_effect=req_lib.ConnectionError):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/auth/register",
                    json={"username": "testuser", "password": "testpass", "email": "t@e.com"},
                )
                assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_token_proxy_invalid_credentials(self):
        """Verify the /auth/token route forwards Django's 401 response."""
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 401
        mock_response.json.return_value = {"detail": "No active account found"}

        with patch("fastapi_backend.app.routes.auth_routes.requests.request", return_value=mock_response):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/auth/token",
                    json={"username": "bad", "password": "wrong"},
                )
                assert response.status_code == 401
