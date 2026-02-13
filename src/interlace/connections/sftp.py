"""
SFTP connection for file sync ingestion.

Phase A: SFTP polling + download for sync jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import paramiko


@dataclass(frozen=True)
class SFTPConfig:
    host: str
    port: int = 22
    username: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None
    known_hosts_path: str | None = None
    # Safety/perf knobs
    connect_timeout_s: float = 15.0


class SFTPConnection:
    """
    Minimal SFTP connection wrapper for polling and downloading remote files.

    This is intentionally storage-focused (not SQL). It is used by `interlace.sync`.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        self.name = name
        self.config = config
        self._transport: paramiko.Transport | None = None
        self._client: paramiko.SFTPClient | None = None

    def _parse_config(self) -> SFTPConfig:
        cfg = self.config.get("config", {}) if isinstance(self.config, dict) else {}
        return SFTPConfig(
            host=cfg.get("host", ""),
            port=int(cfg.get("port", 22)),
            username=cfg.get("username"),
            password=cfg.get("password"),
            private_key_path=cfg.get("private_key_path"),
            private_key_passphrase=cfg.get("private_key_passphrase"),
            known_hosts_path=cfg.get("known_hosts_path"),
            connect_timeout_s=float(cfg.get("connect_timeout_s", 15.0)),
        )

    def connect(self) -> paramiko.SFTPClient:
        """Connect (lazy) and return a live `paramiko.SFTPClient`."""
        if self._client is not None:
            return self._client

        cfg = self._parse_config()
        if not cfg.host:
            raise ValueError(f"SFTP connection '{self.name}' missing host")

        transport = paramiko.Transport((cfg.host, cfg.port))
        transport.banner_timeout = cfg.connect_timeout_s
        transport.auth_timeout = cfg.connect_timeout_s
        transport.connect_timeout = cfg.connect_timeout_s

        pkey = None
        if cfg.private_key_path:
            # Try common key types; paramiko will raise if incompatible.
            try:
                pkey = paramiko.RSAKey.from_private_key_file(cfg.private_key_path, password=cfg.private_key_passphrase)
            except Exception:
                pkey = paramiko.Ed25519Key.from_private_key_file(
                    cfg.private_key_path, password=cfg.private_key_passphrase
                )

        transport.connect(
            username=cfg.username,
            password=cfg.password,
            pkey=pkey,
        )

        self._transport = transport
        self._client = paramiko.SFTPClient.from_transport(transport)
        return self._client

    def close(self) -> None:
        """Close SFTP client + underlying transport."""
        try:
            if self._client is not None:
                self._client.close()
        finally:
            self._client = None
        try:
            if self._transport is not None:
                self._transport.close()
        finally:
            self._transport = None

    def __enter__(self) -> SFTPConnection:
        self.connect()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
        self.close()
