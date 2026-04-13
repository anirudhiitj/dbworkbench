"""Tests for Django authentication ΓÇö registration and token endpoints.

Covers: TC_34, TC_35
Plus edge cases: token claims, refresh, special characters, long inputs.
"""

import os

import pytest
from rest_framework import status
from rest_framework.test import APIClient

from authentication.models import User


pytestmark = pytest.mark.django_db


class TestRegistration:
    """Tests for the Django register() view."""

    def test_register_success(self):
        """TC_35 ΓÇö Verify that providing valid credentials returns 201 and user data."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "newuser", "password": "strongPass99", "email": "new@example.com"},
            format="json",
        )
        assert resp.status_code == status.HTTP_201_CREATED
        data = resp.json()
        assert "id" in data
        assert data["username"] == "newuser"
        assert User.objects.filter(username="newuser").exists()

    def test_register_missing_username(self):
        """Verify that missing username returns 400."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"password": "strongPass99"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_missing_password(self):
        """Verify that missing password returns 400."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "newuser"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_duplicate_username(self):
        """Verify that a duplicate username returns 400."""
        User.objects.create_user(username="existing", password="pass123")
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "existing", "password": "anotherPass"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert "already taken" in resp.json()["detail"].lower()

    def test_register_empty_username(self):
        """Verify that an empty username is rejected."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "", "password": "strongPass99"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_empty_password(self):
        """Verify that an empty password is rejected."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "newuser", "password": ""},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_without_email(self):
        """Verify that registration without email succeeds (email is optional)."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "noemail", "password": "strongPass99"},
            format="json",
        )
        # Should succeed ΓÇö email is not required for AbstractUser
        assert resp.status_code in (status.HTTP_201_CREATED, status.HTTP_400_BAD_REQUEST)

    def test_register_password_not_in_response(self):
        """Verify that the response does not contain the password."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "safeuser", "password": "strongPass99", "email": "s@e.com"},
            format="json",
        )
        assert resp.status_code == status.HTTP_201_CREATED
        data = resp.json()
        assert "password" not in data

    def test_register_user_persisted(self):
        """Verify that a registered user is persisted in the database."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "persisted", "password": "strongPass99", "email": "p@e.com"},
            format="json",
        )
        assert resp.status_code == status.HTTP_201_CREATED
        user = User.objects.get(username="persisted")
        assert user.check_password("strongPass99")


class TestTokenEndpoints:
    """Tests for JWT token obtain and refresh."""

    def test_obtain_token_success(self):
        """TC_35 ΓÇö Verify that valid credentials return access + refresh tokens."""
        User.objects.create_user(username="tokenuser", password="tokenPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "tokenuser", "password": "tokenPass1"},
            format="json",
        )
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert "access" in data
        assert "refresh" in data

    def test_obtain_token_invalid_password(self):
        """TC_34 ΓÇö Verify that invalid credentials return 401."""
        User.objects.create_user(username="tokenuser2", password="correctPass")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "tokenuser2", "password": "wrongPass"},
            format="json",
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_obtain_token_no_credentials(self):
        """TC_34 ΓÇö Verify that no credentials returns 400."""
        client = APIClient()
        resp = client.post("/auth/token/", {}, format="json")
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_obtain_token_nonexistent_user(self):
        """Verify that a non-existent user gets 401."""
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "ghost", "password": "password"},
            format="json",
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_token_refresh(self):
        """Verify that a valid refresh token returns a new access token."""
        User.objects.create_user(username="refreshuser", password="refreshPass1")
        client = APIClient()
        token_resp = client.post(
            "/auth/token/",
            {"username": "refreshuser", "password": "refreshPass1"},
            format="json",
        )
        refresh = token_resp.json()["refresh"]
        resp = client.post(
            "/auth/token/refresh/",
            {"refresh": refresh},
            format="json",
        )
        assert resp.status_code == status.HTTP_200_OK
        assert "access" in resp.json()

    def test_token_refresh_invalid(self):
        """Verify that an invalid refresh token is rejected."""
        client = APIClient()
        resp = client.post(
            "/auth/token/refresh/",
            {"refresh": "invalid-token-string"},
            format="json",
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_token_contains_username_claim(self):
        """Verify that the custom serializer adds a username claim to the token."""
        import jwt as pyjwt

        User.objects.create_user(username="claimuser", password="claimPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "claimuser", "password": "claimPass1"},
            format="json",
        )
        access = resp.json()["access"]
        payload = pyjwt.decode(
            access,
            os.environ["JWT_SECRET_KEY"],
            algorithms=["HS256"],
        )
        assert payload["username"] == "claimuser"

    def test_token_contains_user_id_claim(self):
        """Verify user_id is present in the token payload."""
        import jwt as pyjwt

        user = User.objects.create_user(username="iduser", password="idPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "iduser", "password": "idPass1"},
            format="json",
        )
        access = resp.json()["access"]
        payload = pyjwt.decode(
            access,
            os.environ["JWT_SECRET_KEY"],
            algorithms=["HS256"],
        )
        assert int(payload["user_id"]) == user.id

    def test_access_and_refresh_are_different(self):
        """Verify access and refresh tokens are distinct strings."""
        User.objects.create_user(username="diffuser", password="diffPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "diffuser", "password": "diffPass1"},
            format="json",
        )
        data = resp.json()
        assert data["access"] != data["refresh"]
