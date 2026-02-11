"""
Error handling middleware.

Provides consistent error responses and request ID tracking.
"""

import json
import uuid
import traceback
from typing import Callable

from aiohttp import web

from interlace.service.api.errors import APIError, ErrorCode
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.middleware.error")


@web.middleware
async def error_middleware(
    request: web.Request, handler: Callable
) -> web.Response:
    """
    Middleware for consistent error handling.

    - Adds request_id to all requests
    - Catches APIError and returns structured JSON response
    - Catches unexpected errors and returns generic 500
    - Logs all errors with context
    """
    # Generate request ID
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    request["request_id"] = request_id

    try:
        response = await handler(request)
        # Add request ID to successful responses
        response.headers["X-Request-ID"] = request_id
        return response

    except APIError as e:
        logger.warning(
            f"API error: {e.code.value} - {e.message}",
            extra={
                "request_id": request_id,
                "error_code": e.code.value,
                "status": e.status,
                "path": request.path,
                "method": request.method,
            },
        )
        return web.json_response(
            e.to_dict(request_id),
            status=e.status,
            headers={"X-Request-ID": request_id},
        )

    except json.JSONDecodeError as e:
        logger.warning(
            f"JSON decode error: {e}",
            extra={"request_id": request_id, "path": request.path},
        )
        return web.json_response(
            {
                "error": {
                    "code": ErrorCode.INVALID_REQUEST.value,
                    "message": "Invalid JSON in request body",
                    "request_id": request_id,
                }
            },
            status=400,
            headers={"X-Request-ID": request_id},
        )

    except web.HTTPException:
        # Let aiohttp handle its own HTTP exceptions
        raise

    except Exception as e:
        # Log full traceback for unexpected errors
        logger.error(
            f"Unexpected error: {e}",
            extra={
                "request_id": request_id,
                "path": request.path,
                "method": request.method,
                "traceback": traceback.format_exc(),
            },
            exc_info=True,
        )
        return web.json_response(
            {
                "error": {
                    "code": ErrorCode.INTERNAL_ERROR.value,
                    "message": "An internal error occurred",
                    "request_id": request_id,
                }
            },
            status=500,
            headers={"X-Request-ID": request_id},
        )
