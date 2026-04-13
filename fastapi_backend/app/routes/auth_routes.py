"""Auth proxy routes — forward login, refresh, and register to Django.

The frontend talks only to FastAPI (port 8001).  These routes proxy
auth requests to Django so the frontend never needs to know Django's port.
"""

import requests
from fastapi import APIRouter, HTTPException

from fastapi_backend.app.config import DJANGO_BASE_URL
from fastapi_backend.app.models.schemas import (
    TokenObtainRequest,
    TokenRefreshRequest,
    RegisterRequest,
    RegisterResponse,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


def _proxy(method: str, path: str, payload: dict) -> dict:
    """Forward a request to Django and return the JSON response."""
    url = f"{DJANGO_BASE_URL}{path}"
    try:
        response = requests.request(method, url, json=payload, timeout=10)
    except requests.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot reach Django auth service at {DJANGO_BASE_URL}",
        )
    if not response.ok:
        body = response.json()
        # Flatten Django's {"detail": "..."} so we don't double-nest it
        detail = body.get("detail", body)
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


@router.post("/token")
def obtain_token(request: TokenObtainRequest):
    """Obtain an access + refresh token pair (proxied to Django).

    Returns:
        access  — short-lived JWT (15 min), used in Authorization header
        refresh — long-lived JWT (7 days), used to get a new access token
    """
    return _proxy("POST", "/auth/token/", {
        "username": request.username,
        "password": request.password,
    })


@router.post("/token/refresh")
def refresh_token(request: TokenRefreshRequest):
    """Exchange a refresh token for a new access token (proxied to Django).

    Call this when a request returns 401 with "Token has expired".
    """
    return _proxy("POST", "/auth/token/refresh/", {"refresh": request.refresh})


@router.post("/register", response_model=RegisterResponse, status_code=201)
def register(request: RegisterRequest):
    """Create a new user account (proxied to Django).

    Returns the new user's id and username.
    After registering, call POST /auth/token to get tokens.
    """
    return _proxy("POST", "/auth/register/", {
        "username": request.username,
        "password": request.password,
        "email": request.email,
    })
