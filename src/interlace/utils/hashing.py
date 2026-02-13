"""
File hashing utilities for change detection.

Calculates SHA256 hashes of model files to detect changes.
Supports both sync and async file I/O.
"""

import hashlib
from pathlib import Path
from typing import Any

import aiofiles

from interlace.utils.logging import get_logger

logger = get_logger("interlace.utils.hashing")


def calculate_file_hash(file_path: Path) -> str:
    """
    Calculate SHA256 hash of file content (synchronous).

    Args:
        file_path: Path to file

    Returns:
        SHA256 hash as hex string
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to calculate hash for {file_path}: {e}")
        return ""


async def calculate_file_hash_async(file_path: Path) -> str:
    """
    Calculate SHA256 hash of file content (async).

    Uses aiofiles for non-blocking file I/O, improving performance
    when hashing multiple files in parallel.

    Args:
        file_path: Path to file

    Returns:
        SHA256 hash as hex string
    """
    sha256_hash = hashlib.sha256()
    try:
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(4096)
                if not chunk:
                    break
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to calculate hash for {file_path}: {e}")
        return ""


def get_stored_file_hash(connection: Any, model_name: str, schema_name: str) -> str | None:
    """
    Get stored file hash from database.

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name

    Returns:
        Stored hash or None if not found
    """

    try:
        # Use parameterized query to prevent SQL injection
        # Note: ibis.sql() doesn't support parameters directly, so we use proper escaping
        from interlace.core.state import _escape_sql_string

        safe_model_name = _escape_sql_string(model_name)
        safe_schema_name = _escape_sql_string(schema_name)
        query = f"""
            SELECT file_hash
            FROM interlace.model_file_hashes
            WHERE model_name = '{safe_model_name}' AND schema_name = '{safe_schema_name}'
        """
        result = connection.sql(query).execute()
        if result is not None and len(result) > 0:
            return result.iloc[0]["file_hash"] if hasattr(result, "iloc") else None
        return None
    except Exception as e:
        # Graceful degradation: if table doesn't exist, return None
        error_str = str(e).lower()
        if "does not exist" in error_str or "not found" in error_str or "no such table" in error_str:
            logger.debug(f"File hash table not found: {e}")
        else:
            logger.debug(f"Could not get stored file hash for {model_name}: {e}")
        return None


def store_file_hash(
    connection: Any,
    model_name: str,
    schema_name: str,
    file_path: str,
    file_hash: str,
) -> None:
    """
    Store file hash in database.

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name
        file_path: File path
        file_hash: File hash
    """
    from interlace.core.context import _execute_sql_internal

    try:
        from interlace.core.state import _escape_sql_string

        safe_model_name = _escape_sql_string(model_name)
        safe_schema_name = _escape_sql_string(schema_name)
        safe_file_path = _escape_sql_string(file_path)
        safe_file_hash = _escape_sql_string(file_hash)

        # Delete existing entry if it exists
        delete_query = f"""
            DELETE FROM interlace.model_file_hashes
            WHERE model_name = '{safe_model_name}' AND schema_name = '{safe_schema_name}'
        """
        _execute_sql_internal(connection, delete_query)

        # Insert new entry
        insert_query = f"""
            INSERT INTO interlace.model_file_hashes
            (model_name, schema_name, file_path, file_hash, last_updated_at)
            VALUES ('{safe_model_name}', '{safe_schema_name}', '{safe_file_path}', '{safe_file_hash}', CURRENT_TIMESTAMP)
        """
        _execute_sql_internal(connection, insert_query)
    except Exception as e:
        # Graceful degradation: if table doesn't exist, log as debug
        error_str = str(e).lower()
        if "does not exist" in error_str or "not found" in error_str or "no such table" in error_str:
            logger.debug(f"File hash table not found: {e}")
        else:
            logger.warning(f"Could not store file hash for {model_name}: {e}")


def has_file_changed(connection: Any, model_name: str, schema_name: str, file_path: Path) -> bool:
    """
    Check if file hash has changed (synchronous).

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name
        file_path: Path to file

    Returns:
        True if file has changed or doesn't exist in database, False otherwise
    """
    current_hash = calculate_file_hash(file_path)
    if not current_hash:
        return True  # If we can't calculate hash, assume changed

    stored_hash = get_stored_file_hash(connection, model_name, schema_name)
    if stored_hash is None:
        return True  # Not in database, assume changed

    return current_hash != stored_hash


async def has_file_changed_async(connection: Any, model_name: str, schema_name: str, file_path: Path) -> bool:
    """
    Check if file hash has changed (async).

    Uses async file I/O for better performance when checking multiple files.

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name
        file_path: Path to file

    Returns:
        True if file has changed or doesn't exist in database, False otherwise
    """
    current_hash = await calculate_file_hash_async(file_path)
    if not current_hash:
        return True  # If we can't calculate hash, assume changed

    stored_hash = get_stored_file_hash(connection, model_name, schema_name)
    if stored_hash is None:
        return True  # Not in database, assume changed

    return current_hash != stored_hash
