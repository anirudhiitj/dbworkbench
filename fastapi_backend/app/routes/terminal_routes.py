"""WebSocket virtual psql terminal.

Provides an interactive terminal experience over WebSocket backed by a real
psycopg2 connection to the user's external database.  Authentication uses a
short-lived, single-use ticket obtained via the /terminal/ticket endpoint,
avoiding JWT exposure in WebSocket query strings (which can appear in logs).
"""

import asyncio
import logging
import secrets
import time
from typing import Optional

import jwt
import psycopg2
from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.config import JWT_ALGORITHM, JWT_SECRET_KEY
from connections.models import ConnectionProfile

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/terminal", tags=["Terminal"])

_PROMPT = "\r\n\x1b[1;32m{db}=# \x1b[0m"

# In-memory store for single-use tickets: {ticket: {user_id, connection_profile_id, expires}}
_TICKET_STORE: dict[str, dict] = {}
_TICKET_TTL = 30  # seconds


def _issue_ticket(user_id: int, connection_profile_id: int) -> str:
    """Create a short-lived single-use ticket."""
    # Purge expired tickets
    now = time.monotonic()
    expired = [k for k, v in _TICKET_STORE.items() if v["expires"] < now]
    for k in expired:
        del _TICKET_STORE[k]
    ticket = secrets.token_urlsafe(32)
    _TICKET_STORE[ticket] = {
        "user_id": user_id,
        "connection_profile_id": connection_profile_id,
        "expires": now + _TICKET_TTL,
    }
    return ticket


def _redeem_ticket(ticket: str) -> Optional[dict]:
    """Consume a ticket, returning the payload or None if invalid/expired."""
    data = _TICKET_STORE.pop(ticket, None)
    if data is None:
        return None
    if time.monotonic() > data["expires"]:
        return None
    return data


@router.post("/ticket")
def create_ticket(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Exchange a JWT for a short-lived, single-use WebSocket ticket."""
    ticket = _issue_ticket(current_user["user_id"], connection_profile_id)
    return {"ticket": ticket}


def _fmt(cur) -> str:
    """Format psycopg2 cursor results as a plain-text ASCII table."""
    if cur.description is None:
        msg = cur.statusmessage or "OK"
        return f"\r\n\x1b[32m{msg}\x1b[0m"

    cols = [d[0] for d in cur.description]
    raw_rows = cur.fetchall()
    rows = [[str(v) if v is not None else "NULL" for v in row] for row in raw_rows]

    widths = [len(c) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    hdr = "|" + "|".join(f" {c:<{w}} " for c, w in zip(cols, widths)) + "|"

    lines = [f"\r\n{sep}", f"\r\n{hdr}", f"\r\n{sep}"]
    for row in rows:
        lines.append("\r\n|" + "|".join(f" {v:<{w}} " for v, w in zip(row, widths)) + "|")
    lines.append(f"\r\n{sep}")
    cnt_word = "row" if len(rows) == 1 else "rows"
    lines.append(f"\r\n({len(rows)} {cnt_word})")
    return "".join(lines)


@router.websocket("/ws")
async def terminal_ws(
    websocket: WebSocket,
    ticket: str = Query(...),
):
    await websocket.accept()

    # --- Authenticate via single-use ticket ---
    ticket_data = _redeem_ticket(ticket)
    if not ticket_data:
        await websocket.send_text("\x1b[31mAuthentication failed.\x1b[0m\r\n")
        await websocket.close(code=4001)
        return

    user_id = ticket_data["user_id"]
    connection_profile_id = ticket_data["connection_profile_id"]

    # --- Fetch connection profile ---
    try:
        profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    except ConnectionProfile.DoesNotExist:
        await websocket.send_text("\x1b[31mConnection profile not found.\x1b[0m\r\n")
        await websocket.close(code=4004)
        return

    # --- Open psycopg2 connection ---
    try:
        conn = psycopg2.connect(
            host=profile.host,
            port=profile.port,
            dbname=profile.database_name,
            user=profile.db_username,
            password=profile.get_decrypted_password(),
        )
        conn.autocommit = True
    except Exception as e:
        await websocket.send_text(f"\x1b[31mCannot connect to database: {e}\x1b[0m\r\n")
        await websocket.close(code=4000)
        return

    db = profile.database_name

    # Welcome banner + first prompt
    banner = (
        f"\x1b[1;34mWEAVE-DB Terminal\x1b[0m  "
        f"\x1b[33m{profile.db_username}@{profile.host}:{profile.port}/{db}\x1b[0m\r\n"
        "Type SQL and press Enter.  Type \\q to quit.\r\n"
        + _PROMPT.format(db=db)
    )
    await websocket.send_text(banner)

    buf = ""

    try:
        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive(), timeout=300)
            except asyncio.TimeoutError:
                await websocket.send_text("\r\n\x1b[33mSession timed out.\x1b[0m\r\n")
                break

            if msg["type"] == "websocket.disconnect":
                break

            raw_bytes = msg.get("bytes")
            raw_text = msg.get("text", "")
            data: str = (
                raw_text
                if raw_text
                else (raw_bytes or b"").decode("utf-8", errors="replace")
            )

            if not data:
                continue

            # --- Enter ---
            if data in ("\r", "\n", "\r\n"):
                await websocket.send_text("\r\n")
                cmd = buf.strip()
                buf = ""
                if not cmd:
                    await websocket.send_text(_PROMPT.format(db=db))
                    continue
                if cmd.lower() in ("\\q", "exit", "quit"):
                    await websocket.send_text("\x1b[33mBye.\x1b[0m\r\n")
                    break
                try:
                    cur = conn.cursor()
                    cur.execute(cmd)
                    result = _fmt(cur)
                    cur.close()
                except psycopg2.Error as e:
                    pg_msg = (e.pgerror or str(e)).strip()
                    result = f"\x1b[31mERROR:  {pg_msg}\x1b[0m"
                except Exception as e:
                    result = f"\x1b[31mError: {e}\x1b[0m"
                await websocket.send_text(result + _PROMPT.format(db=db))

            # --- Backspace / Delete ---
            elif data in ("\x7f", "\x08"):
                if buf:
                    buf = buf[:-1]
                    await websocket.send_text("\b \b")

            # --- Ctrl-C ---
            elif data == "\x03":
                buf = ""
                await websocket.send_text("^C" + _PROMPT.format(db=db))

            # --- Ctrl-D (EOF / quit) ---
            elif data == "\x04":
                await websocket.send_text("\r\n\x1b[33mBye.\x1b[0m\r\n")
                break

            # --- Escape sequences (arrow keys, function keys) — Phase 4 ignores ---
            elif data.startswith("\x1b"):
                pass

            # --- Printable text / paste ---
            else:
                printable = "".join(c for c in data if ord(c) >= 32)
                if printable:
                    buf += printable
                    await websocket.send_text(printable)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("Terminal session error: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass
