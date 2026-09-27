"""Scoped API keys for the HTTP service. Only the hash is stored."""

from __future__ import annotations

import hashlib
import json
import secrets

from interlace.exceptions import ConfigurationError
from interlace.state.codec import _now_iso
from interlace.state.conn import ControlDb

_API_SCOPES = frozenset({"read", "write", "admin"})


class ApiKeyStore:
    """API keys on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def create_api_key(self, name: str, scopes: list[str]) -> str:
        """Create a key; returns the plaintext token (shown once — only the hash is stored)."""
        return await self._db.io(self._create_api_key_sync, name, scopes)

    def _create_api_key_sync(self, name: str, scopes: list[str]) -> str:
        cleaned = name.strip()
        if not cleaned:
            raise ConfigurationError("name the key")
        if not scopes or set(scopes) - _API_SCOPES:
            raise ConfigurationError(f"scopes must be a non-empty subset of read/write/admin (got {scopes})")
        token = "ilk_" + secrets.token_hex(16)
        with self._db.lock:
            existing = self._db.conn.execute("SELECT 1 FROM api_keys WHERE name = ?", (cleaned,)).fetchone()
            if existing is not None:
                raise ConfigurationError(
                    f"a key named {cleaned!r} already exists — revoke it first or pick another name"
                )
            self._db.conn.execute(
                "INSERT INTO api_keys (name, key_hash, scopes, created_at) VALUES (?, ?, ?, ?)",
                (cleaned, hashlib.sha256(token.encode()).hexdigest(), json.dumps(list(scopes)), _now_iso()),
            )
            self._db.conn.commit()
        return token

    async def verify_api_key(self, token: str) -> tuple[str, list[str]] | None:
        """Return ``(name, scopes)``, or None if the token is unknown."""
        return await self._db.io(self._verify_api_key_sync, token)

    def _verify_api_key_sync(self, token: str) -> tuple[str, list[str]] | None:
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self._db.lock:
            row = self._db.conn.execute("SELECT name, scopes FROM api_keys WHERE key_hash = ?", (digest,)).fetchone()
        if row is None:
            return None
        scopes = json.loads(row["scopes"])
        return str(row["name"]), list(scopes)

    async def revoke_api_key(self, name: str) -> int:
        """Revoke every key with this name; returns how many were removed."""
        return await self._db.io(self._revoke_api_key_sync, name)

    def _revoke_api_key_sync(self, name: str) -> int:
        with self._db.lock:
            total = int(self._db.conn.execute("SELECT count(*) FROM api_keys").fetchone()[0])
            matching = int(self._db.conn.execute("SELECT count(*) FROM api_keys WHERE name = ?", (name,)).fetchone()[0])
            if matching == 0:
                return 0
            if matching == total:
                # Zero keys means keyless mode: every request is admin.
                raise ConfigurationError(
                    "refusing to revoke the last key(s) — that would disable authentication; create a replacement first"
                )
            removed = self._db.conn.execute("DELETE FROM api_keys WHERE name = ?", (name,)).rowcount
            self._db.conn.commit()
        return removed

    async def count_api_keys(self) -> int:
        return await self._db.io(self._count_api_keys_sync)

    def _count_api_keys_sync(self) -> int:
        with self._db.lock:
            row = self._db.conn.execute("SELECT count(*) FROM api_keys").fetchone()
        return int(row[0])

    async def list_api_keys(self) -> list[dict[str, object]]:
        return await self._db.io(self._list_api_keys_sync)

    def _list_api_keys_sync(self) -> list[dict[str, object]]:
        with self._db.lock:
            rows = self._db.conn.execute("SELECT name, scopes, created_at FROM api_keys ORDER BY id").fetchall()
        return [{"name": r["name"], "scopes": json.loads(r["scopes"]), "created_at": r["created_at"]} for r in rows]
