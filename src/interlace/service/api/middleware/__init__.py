"""
API middleware components.

Provides error handling, CORS, and rate limiting.
"""

from interlace.service.api.middleware.cors import setup_cors
from interlace.service.api.middleware.error import error_middleware

__all__ = ["error_middleware", "setup_cors"]
