"""
API error definitions and exception classes.

Provides consistent error handling across all API endpoints.
"""

from enum import Enum
from typing import Any, Dict, Optional


class ErrorCode(str, Enum):
    """Standard API error codes."""

    # Client errors (4xx)
    INVALID_REQUEST = "INVALID_REQUEST"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"
    FLOW_NOT_FOUND = "FLOW_NOT_FOUND"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND"
    RUN_NOT_FOUND = "RUN_NOT_FOUND"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"  # Generic not-found for columns, etc.
    RATE_LIMITED = "RATE_LIMITED"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"

    # Server errors (5xx)
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"


class APIError(Exception):
    """
    Base API exception with structured error response.

    Usage:
        raise APIError(
            code=ErrorCode.MODEL_NOT_FOUND,
            message="Model 'users' not found",
            status=404,
            details={"model_name": "users"}
        )
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        status: int = 400,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

    def to_dict(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Convert to API response format."""
        error_dict: Dict[str, Any] = {
            "code": self.code.value,
            "message": self.message,
        }
        if self.details:
            error_dict["details"] = self.details
        if request_id:
            error_dict["request_id"] = request_id
        return {"error": error_dict}


class NotFoundError(APIError):
    """Resource not found error."""

    def __init__(
        self,
        resource_type: str,
        resource_id: str,
        code: ErrorCode = ErrorCode.MODEL_NOT_FOUND,
    ):
        super().__init__(
            code=code,
            message=f"{resource_type} '{resource_id}' not found",
            status=404,
            details={f"{resource_type.lower()}_id": resource_id},
        )


class ValidationError(APIError):
    """Request validation error."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code=ErrorCode.VALIDATION_ERROR,
            message=message,
            status=400,
            details=details,
        )


class ExecutionError(APIError):
    """Model execution error."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code=ErrorCode.EXECUTION_ERROR,
            message=message,
            status=500,
            details=details,
        )
