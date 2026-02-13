"""
PGP file decryption processor using PGPy (pure Python).

Phase A: Used in sync jobs to decrypt files (e.g., .pgp/.gpg) before upload.

Notes:
- PGPy generally requires loading the encrypted message into memory; this is not
  ideal for very large files. If you need maximum throughput/streaming, add an
  alternative processor that shells out to system `gpg`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.processors.pgp")


class PGPDecryptProcessor:
    """
    Decrypt an encrypted file using PGP.

    Config (passed via ProcessorSpec.config, or via metadata['processor_config']):
    - private_key_path: str (required)
    - private_key_passphrase: str (optional)
    - output_suffix: str (optional, default: remove .pgp/.gpg/.asc else add '.decrypted')
    """

    name = "pgp_decrypt"

    def process(self, input_path: str, *, metadata: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        cfg = metadata.get("processor_config") or {}
        key_path = cfg.get("private_key_path")
        if not key_path:
            raise ValueError("pgp_decrypt requires processor config: private_key_path")
        passphrase = cfg.get("private_key_passphrase")

        output_path = self._default_output_path(input_path, cfg.get("output_suffix"))
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Import lazily so environments without pgpy can still run other processors.
        import pgpy

        # Load private key
        key, _ = pgpy.PGPKey.from_file(str(key_path))
        if passphrase:
            with key.unlock(passphrase):
                self._decrypt_to_file(pgpy, input_path, output_path, key)
        else:
            self._decrypt_to_file(pgpy, input_path, output_path, key)

        new_meta = dict(metadata)
        new_meta["pgp_decrypted"] = True
        new_meta["output_path"] = str(output_path)
        return str(output_path), new_meta

    def _decrypt_to_file(self, pgpy: Any, input_path: str, output_path: Path, key: Any) -> None:
        message = pgpy.PGPMessage.from_file(str(input_path))
        decrypted = key.decrypt(message)

        # `decrypted.message` can be str (text) or bytes depending on input.
        payload = decrypted.message
        tmp_path = output_path.with_suffix(output_path.suffix + ".part")
        if isinstance(payload, bytes):
            tmp_path.write_bytes(payload)
        else:
            # Write as UTF-8 text; if binary content came through as str, this may corrupt.
            # Users should ensure binary payloads are produced as bytes (or use gpg processor).
            tmp_path.write_text(payload or "", encoding="utf-8")

        os.replace(tmp_path, output_path)

    def _default_output_path(self, input_path: str, output_suffix: str | None) -> Path:
        p = Path(input_path)
        if output_suffix:
            return p.with_name(p.name + output_suffix)

        lowered = p.name.lower()
        for ext in (".pgp", ".gpg", ".asc"):
            if lowered.endswith(ext):
                return p.with_name(p.name[: -len(ext)])

        return p.with_name(p.name + ".decrypted")
