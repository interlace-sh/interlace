"""
Schema utilities for converting fields to ibis schemas.

Handles conversion of native Python types to ibis.Schema using ibis.schema().
"""

from typing import TYPE_CHECKING, Any

import ibis

if TYPE_CHECKING:
    import pyarrow

from interlace.utils.logging import get_logger

logger = get_logger("interlace.utils.schema_utils")


def fields_to_ibis_schema(fields: Any) -> ibis.Schema | None:
    """
    Convert fields parameter to ibis.Schema using ibis.schema().

    Supports native ibis schema formats:
    - Dict: {"x": int, "y": str} or {"x": "int64", "y": "string"}
    - List of tuples: [("foo", "string"), ("bar", "int64")]
    - List of names + types: names=["foo", "bar"], types=["string", "int64"]
    - Existing Schema object (no-op)

    Args:
        fields: Fields in any supported format (dict, list of tuples, Schema, etc.)

    Returns:
        ibis.Schema object, or None if fields is None/empty

    Examples:
        >>> fields_to_ibis_schema({"x": int, "y": str})
        ibis.Schema { x: int64, y: string }

        >>> fields_to_ibis_schema([("foo", "string"), ("bar", "int64")])
        ibis.Schema { foo: string, bar: int64 }

        >>> fields_to_ibis_schema({"x": "int64", "y": "string"})
        ibis.Schema { x: int64, y: string }
    """
    if fields is None:
        return None

    # If already an ibis.Schema, return as-is
    if isinstance(fields, ibis.Schema):
        return fields

    try:
        # Use ibis.schema() to handle all native formats
        # It accepts: dict, list of tuples, names+types, or existing Schema
        return ibis.schema(fields)
    except Exception as e:
        logger.warning(f"Failed to convert fields to ibis schema: {e}. Fields: {fields}")
        return None


def merge_schemas(
    base_schema: ibis.Schema,
    fields_schema: ibis.Schema | None,
) -> ibis.Schema:
    """
    Merge fields schema into base schema (add/replace fields).

    If fields_schema is provided, it adds/replaces those fields in the base schema.
    Fields in fields_schema take precedence over base_schema.

    Args:
        base_schema: Base schema (from inferred data)
        fields_schema: Optional fields schema (from fields parameter)

    Returns:
        Merged ibis.Schema with fields added/replaced
    """
    if fields_schema is None:
        return base_schema

    # Start with base schema
    merged_dict = dict(base_schema)

    # Add/replace with fields schema
    for field_name, field_type in fields_schema.items():
        merged_dict[field_name] = field_type

    # Create new schema from merged dict
    return ibis.schema(merged_dict)


def apply_schema_to_dataframe(df: Any, schema: ibis.Schema) -> Any:
    """
    Apply ibis schema to pandas DataFrame by casting columns in-place.
    Avoids re-executing queries by directly casting DataFrame columns.

    This is a performance optimization to eliminate the pattern:
        data.execute() → DataFrame → ibis.memtable() → data.execute()

    Instead, we:
        data.execute() → DataFrame → apply_schema_to_dataframe()

    Args:
        df: pandas DataFrame to cast
        schema: ibis.Schema with target types

    Returns:
        pandas DataFrame with columns cast to target types

    Note:
        Uses pyarrow for type mapping. If casting fails for a column,
        the original type is preserved (no error raised).
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        return df

    # Convert ibis schema to pyarrow schema
    try:
        arrow_schema = schema.to_pyarrow()
    except Exception as e:
        logger.warning(f"Could not convert schema to PyArrow: {e}")
        return df

    # Cast each column according to schema
    for field in arrow_schema:
        col_name = field.name
        if col_name in df.columns:
            try:
                pandas_dtype = _arrow_to_pandas_dtype(field.type)
                if pandas_dtype:
                    df[col_name] = df[col_name].astype(pandas_dtype)
            except Exception as e:
                # Keep original type if cast fails
                logger.debug(f"Could not cast column {col_name} to {field.type}: {e}")
                pass

    return df


def _arrow_to_pandas_dtype(arrow_type: "pyarrow.DataType") -> str | None:
    """
    Map PyArrow type to pandas dtype string.

    Args:
        arrow_type: PyArrow data type

    Returns:
        pandas dtype string, or None if no mapping exists
    """
    import pyarrow.types as pat

    # Numeric types
    if pat.is_int8(arrow_type):
        return "int8"
    elif pat.is_int16(arrow_type):
        return "int16"
    elif pat.is_int32(arrow_type):
        return "int32"
    elif pat.is_int64(arrow_type):
        return "int64"
    elif pat.is_uint8(arrow_type):
        return "uint8"
    elif pat.is_uint16(arrow_type):
        return "uint16"
    elif pat.is_uint32(arrow_type):
        return "uint32"
    elif pat.is_uint64(arrow_type):
        return "uint64"
    elif pat.is_float16(arrow_type):
        return "float16"
    elif pat.is_float32(arrow_type):
        return "float32"
    elif pat.is_float64(arrow_type):
        return "float64"
    # Boolean
    elif pat.is_boolean(arrow_type):
        return "bool"
    # String types
    elif pat.is_string(arrow_type) or pat.is_unicode(arrow_type) or pat.is_utf8(arrow_type):
        return "str"
    # Binary
    elif pat.is_binary(arrow_type):
        return "object"
    # Date/Time types
    elif pat.is_date(arrow_type):
        return "datetime64[ns]"
    elif pat.is_timestamp(arrow_type):
        return "datetime64[ns]"
    elif pat.is_time(arrow_type):
        return "object"  # pandas doesn't have native time type
    # Decimal
    elif pat.is_decimal(arrow_type):
        return "object"  # pandas decimal as object
    # Default
    else:
        return "object"
