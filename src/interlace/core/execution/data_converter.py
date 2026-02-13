"""
Data conversion utilities for converting Python data structures to ibis tables.

Handles conversion of various Python data types (dicts, lists, DataFrames)
to ibis.Table using ibis.memtable().

Includes smart type coercion:
- Nested dicts → struct types
- Homogeneous arrays → array types
- Heterogeneous arrays → JSON (automatic fallback for 'unknown' types)
"""

import json
from typing import Any

import ibis
import ibis.expr.datatypes as dt

from interlace.utils.logging import get_logger
from interlace.utils.schema_utils import fields_to_ibis_schema, merge_schemas

logger = get_logger("interlace.execution.data_converter")


class DataConverter:
    """
    Converts Python data structures to ibis.Table.

    Handles conversion of various Python data types (dicts, lists, DataFrames)
    to ibis.Table using ibis.memtable().

    Smart type coercion:
    - Nested dicts automatically become struct<...> types
    - Homogeneous arrays become array<type> types
    - Heterogeneous arrays (mixed types) are converted to JSON
    """

    @staticmethod
    def _coerce_unknown_to_json(
        data_list: list[dict[str, Any]],
        schema: ibis.Schema,
    ) -> tuple[list[dict[str, Any]], ibis.Schema]:
        """
        Detect columns with 'unknown' type and convert them to JSON strings.

        This handles heterogeneous arrays like [1, "two", 3.0] which ibis
        can't infer a type for. We serialize them to JSON for storage.

        Args:
            data_list: List of dicts (original data)
            schema: Inferred schema from ibis.memtable()

        Returns:
            Tuple of (modified data_list, fixed schema)
        """
        unknown_cols = []
        for col_name, col_type in schema.items():
            # Check if type is 'unknown' or 'null'
            type_str = str(col_type).lower()
            if "unknown" in type_str or type_str == "null":
                unknown_cols.append(col_name)

        if not unknown_cols:
            return data_list, schema

        logger.debug(f"Converting columns with unknown types to JSON: {unknown_cols}")

        # Convert unknown columns to JSON strings
        new_data = []
        for row in data_list:
            new_row = dict(row)
            for col in unknown_cols:
                if col in new_row and new_row[col] is not None:
                    new_row[col] = json.dumps(new_row[col])
            new_data.append(new_row)

        # Build new schema with unknown columns as string (JSON)
        new_schema_dict = {}
        for col_name, col_type in schema.items():
            if col_name in unknown_cols:
                new_schema_dict[col_name] = dt.JSON()
            else:
                new_schema_dict[col_name] = col_type

        return new_data, ibis.schema(new_schema_dict)

    @staticmethod
    def apply_column_mapping(
        data: list[dict[str, Any]],
        column_mapping: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Apply column renames to list of dicts.

        Args:
            data: List of dicts to transform
            column_mapping: Dict of {old_name: new_name} for renames

        Returns:
            Transformed list of dicts with columns renamed
        """
        if not data or not column_mapping:
            return data

        result = []
        for row in data:
            new_row = {}
            for col, value in row.items():
                # Apply rename if applicable
                new_col = column_mapping.get(col, col)
                new_row[new_col] = value
            result.append(new_row)

        return result

    @staticmethod
    def convert_to_ibis_table(
        data: Any,
        fields: dict[str, Any] | list | ibis.Schema | None = None,
        strict: bool = False,
        column_mapping: dict[str, str] | None = None,
    ) -> ibis.Table:
        """
        Convert Python data structures to ibis Table using ibis.memtable().

        Supports:
        - List of dicts: [{"col1": val1, "col2": val2}, ...] - converted via ibis.memtable()
        - Dict of lists: {"col1": [val1, val2], "col2": [val3, val4]} - converted via ibis.memtable()
        - pandas DataFrame - converted via ibis.memtable()
        - Generators (yield): Functions that use yield will have all yielded values collected
        - JSON-like structures
        - Single dict (treated as single row) - wrapped in list then converted
        - ibis.Table (returned as-is)

        Args:
            data: Python data structure (dict, list, DataFrame, generator, ibis.Table, etc.)
            fields: Optional schema definition in native ibis format (dict, list of tuples, or Schema)
                   Examples: {"x": int, "y": str} or [("foo", "string"), ("bar", "int64")]
            strict: If True, only output columns specified in fields (drop extras).
                   If False (default), merge fields with inferred schema (keep extras).
            column_mapping: Optional dict for column renames: {"old_name": "new_name"}

        Returns:
            ibis.Table: Converted ibis table
        """
        if isinstance(data, ibis.Table):
            # For ibis.Table, apply column mapping via select/rename if needed
            if column_mapping:
                data = DataConverter._apply_mapping_to_ibis_table(data, column_mapping)
            if strict and fields:
                # Select only specified columns
                fields_schema = fields_to_ibis_schema(fields)
                if fields_schema:
                    data = data.select(*[data[col] for col in fields_schema.names if col in data.columns])
            return data

        import types

        if isinstance(data, types.GeneratorType):
            data = list(data)

        # ibis.memtable() accepts lists of dicts directly, so convert everything to that format
        if isinstance(data, dict):
            # Check if dict of lists (like {"col1": [val1, val2]})
            # Must check ALL values are lists, not just the first one.
            # If only the first value is a list but others are strings/scalars,
            # indexing into non-list values silently corrupts data.
            if data and all(isinstance(v, list) for v in data.values()):
                # Convert dict of lists to list of dicts for ibis.memtable()
                keys = list(data.keys())
                lengths = {len(data[k]) for k in keys if isinstance(data[k], list)}
                if len(lengths) != 1:
                    raise ValueError("Dict of lists must have same length for all lists")
                length = lengths.pop() if lengths else 0
                data_list = [{k: data[k][i] for k in keys} for i in range(length)]
            else:
                # Single dict - wrap in list
                data_list = [data]
        elif isinstance(data, list):
            if not data:
                # Empty list - can only create table if fields schema is provided
                if fields:
                    data_list = []  # Will use fields_schema below
                else:
                    raise ValueError(
                        "Cannot convert empty list to ibis table without 'fields' schema. "
                        "Either provide at least one row, or specify 'fields' parameter."
                    )
            elif isinstance(data[0], dict):
                # List of dicts - use directly
                data_list = data
            else:
                raise ValueError(
                    "List of non-dict values not supported. " 'Use list of dicts like [{"col1": val1}, {"col1": val2}]'
                )
        else:
            # For pandas DataFrame or other types, convert to list of dicts first
            try:
                import pandas as pd

                if isinstance(data, pd.DataFrame):
                    if data.empty:
                        # Empty DataFrame - can only create table if fields schema is provided
                        if fields:
                            data_list = []  # Will use fields_schema below
                        else:
                            raise ValueError(
                                "Cannot convert empty DataFrame to ibis table without 'fields' schema. "
                                "Either provide at least one row, or specify 'fields' parameter."
                            )
                    else:
                        data_list = data.to_dict("records")
                else:
                    raise ValueError(
                        f"Unsupported return type: {type(data)}. "
                        f"Supported types: dict, list of dicts, pandas.DataFrame, ibis.Table, generator (yield)"
                    )
            except ImportError as e:
                raise ValueError(
                    f"Unsupported return type: {type(data)}. "
                    f"Supported types: dict, list of dicts, ibis.Table, generator (yield). "
                    f"For pandas.DataFrame support, install pandas."
                ) from e

        # Apply column mapping (renames) before creating table
        if column_mapping:
            data_list = DataConverter.apply_column_mapping(data_list, column_mapping)

        # Convert fields to ibis schema if provided
        fields_schema = fields_to_ibis_schema(fields)

        if strict and fields_schema:
            # STRICT mode: only output columns specified in fields
            # Filter data to only include specified columns
            specified_cols = set(fields_schema.names)
            filtered_data = [{k: v for k, v in row.items() if k in specified_cols} for row in data_list]
            return ibis.memtable(filtered_data, schema=fields_schema)
        elif fields_schema:
            # Handle empty data with schema
            if not data_list:
                return ibis.memtable([], schema=fields_schema)

            # MERGE mode (default): add/replace fields to inferred schema, keep extra columns
            inferred_table = ibis.memtable(data_list)
            inferred_schema = inferred_table.schema()

            # Smart coercion: convert unknown types to JSON
            data_list, inferred_schema = DataConverter._coerce_unknown_to_json(data_list, inferred_schema)

            merged_schema = merge_schemas(inferred_schema, fields_schema)
            return ibis.memtable(data_list, schema=merged_schema)
        else:
            # No fields specified - just infer schema
            inferred_table = ibis.memtable(data_list)
            inferred_schema = inferred_table.schema()

            # Smart coercion: convert unknown types to JSON
            data_list, fixed_schema = DataConverter._coerce_unknown_to_json(data_list, inferred_schema)

            # Only recreate if schema was fixed
            if fixed_schema != inferred_schema:
                return ibis.memtable(data_list, schema=fixed_schema)
            return inferred_table

    @staticmethod
    def _apply_mapping_to_ibis_table(
        table: ibis.Table,
        column_mapping: dict[str, str],
    ) -> ibis.Table:
        """
        Apply column renames to an ibis.Table using ibis operations.

        Args:
            table: ibis.Table to transform
            column_mapping: Dict of {old_name: new_name} for renames

        Returns:
            Transformed ibis.Table
        """
        if not column_mapping:
            return table

        columns = list(table.columns)
        select_exprs = []

        for col in columns:
            new_name = column_mapping.get(col, col)
            if new_name != col:
                select_exprs.append(table[col].name(new_name))
            else:
                select_exprs.append(table[col])

        if select_exprs:
            return table.select(*select_exprs)
        return table

    @staticmethod
    def extract_count_from_result(result: Any) -> int | None:
        """
        Extract count value from SQL query result.

        Handles various result formats:
        - DataFrame with 'count' column
        - Scalar value
        - List/tuple with count
        - ibis Table result

        Args:
            result: SQL query result (format depends on backend)

        Returns:
            Count value or None if unable to extract
        """
        if result is None:
            return None

        try:
            # Try pandas DataFrame (common for DuckDB)
            if hasattr(result, "iloc") and hasattr(result, "columns"):
                if "count" in result.columns:
                    return int(result.iloc[0]["count"])
                elif len(result.columns) == 1:
                    return int(result.iloc[0, 0])

            # Try scalar value
            if isinstance(result, (int, float)):
                return int(result)

            # Try list/tuple
            if isinstance(result, (list, tuple)):
                if len(result) > 0:
                    if isinstance(result[0], (list, tuple)):
                        return int(result[0][0]) if len(result[0]) > 0 else None
                    else:
                        return int(result[0])

            # Try dict
            if isinstance(result, dict):
                if "count" in result:
                    return int(result["count"])
                elif len(result) == 1:
                    return int(list(result.values())[0])

            # Try to convert to int directly
            try:
                return int(result)
            except (ValueError, TypeError):
                pass

            return None
        except Exception:
            return None
