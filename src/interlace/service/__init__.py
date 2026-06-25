"""HTTP service layer (Litestar). Requires the ``service`` extra."""

from __future__ import annotations

from interlace.service.app import create_app

__all__ = ["create_app"]
