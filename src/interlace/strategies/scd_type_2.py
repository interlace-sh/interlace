"""
SCD Type 2 Strategy - Slowly Changing Dimension with history tracking.

Implements hash-based change detection for maintaining dimension history:
- Tracks all historical versions of records
- Uses hash comparison to detect changes in tracked columns
- Supports configurable column names and delete modes

Industry-aligned implementation following dbt snapshots, Delta Lake, and SQLMesh patterns.
"""

from interlace.strategies.base import Strategy
from typing import List, Optional, Dict, Any, TYPE_CHECKING
import ibis
from interlace.utils.sql_escape import escape_identifier, escape_qualified_name
from interlace.utils.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("interlace.strategies.scd_type_2")


# Default configuration for SCD Type 2
DEFAULT_SCD2_CONFIG = {
    "valid_from_col": "valid_from",
    "valid_to_col": "valid_to",
    "is_current_col": "is_current",
    "hash_col": "_scd2_hash",
    "tracked_columns": None,  # None means track all non-SCD2 columns
    "delete_mode": "ignore",  # "ignore", "soft", "soft_mark"
    "deleted_col": "is_deleted",  # Only used if delete_mode="soft_mark"
}


class SCDType2Strategy(Strategy):
    """
    SCD Type 2 Strategy - maintains full history of dimension changes.

    Change Detection Methods:
    - Hash-based (default): MD5 hash of tracked columns detects any change
    - Timestamp-based: Compare updated_at column (when source has reliable timestamps)

    Usage:
        @model(
            name="dim_customer",
            strategy="scd_type_2",
            primary_key="customer_id",
            scd2_config={
                "tracked_columns": ["name", "email", "address"],  # Optional
                "valid_from_col": "valid_from",
                "valid_to_col": "valid_to",
                "is_current_col": "is_current",
                "delete_mode": "soft",
            }
        )
        def dim_customer(stg: ibis.Table) -> ibis.Table:
            return stg.select(['customer_id', 'name', 'email', 'address'])

    Generated Table Structure:
        - All source columns
        - valid_from (TIMESTAMP): When record became active
        - valid_to (TIMESTAMP): When record was superseded (NULL = current)
        - is_current (BOOLEAN): Current record indicator
        - _scd2_hash (VARCHAR): Hash of tracked columns for change detection
    """

    def generate_sql(
        self,
        connection: ibis.BaseBackend,
        target_table: str,
        schema: str,
        source_table: str,
        primary_key: str | List[str],
        scd2_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """
        Generate SCD Type 2 MERGE SQL statement.

        The SQL performs:
        1. For matching records with changed hash: Close old record, insert new version
        2. For new records: Insert with is_current=TRUE
        3. For deleted records (if delete_mode="soft"): Close the record

        Args:
            connection: ibis connection backend
            target_table: Target dimension table name
            schema: Schema/database name
            source_table: Source table name (temp table with new data)
            primary_key: Business key column(s) for matching
            scd2_config: SCD2 configuration dict
            **kwargs: Additional parameters

        Returns:
            SQL statements to execute (may be multiple statements)
        """
        if not primary_key:
            raise ValueError("primary_key required for scd_type_2 strategy")

        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Merge config with defaults
        config = {**DEFAULT_SCD2_CONFIG, **(scd2_config or {})}

        valid_from_col = config["valid_from_col"]
        valid_to_col = config["valid_to_col"]
        is_current_col = config["is_current_col"]
        hash_col = config["hash_col"]
        tracked_columns = config["tracked_columns"]
        delete_mode = config["delete_mode"]
        deleted_col = config["deleted_col"]

        # Get column names from source table
        try:
            source = connection.table(source_table)
            source_schema = source.schema()
            all_columns = list(source_schema.keys())
        except Exception as e:
            raise ValueError(f"Cannot get schema for source table {source_table}: {e}")

        # Determine SCD2 metadata columns (these shouldn't be in source data)
        scd2_meta_cols = {valid_from_col, valid_to_col, is_current_col, hash_col, deleted_col}

        # Source columns are everything except SCD2 metadata
        source_columns = [col for col in all_columns if col not in scd2_meta_cols]

        # Tracked columns for hash comparison (default: all non-key source columns)
        if tracked_columns is None:
            tracked_columns = [col for col in source_columns if col not in primary_key]

        # Validate tracked columns exist
        missing_cols = set(tracked_columns) - set(source_columns)
        if missing_cols:
            raise ValueError(f"Tracked columns not found in source: {missing_cols}")

        # Build hash expression for change detection
        hash_expr = self._build_hash_expression(tracked_columns, "source")

        # Build ON condition for matching business key with current records
        on_conditions = " AND ".join([
            f"target.{escape_identifier(pk)} = source.{escape_identifier(pk)}"
            for pk in primary_key
        ])

        # Build column lists with proper escaping
        escaped_source_cols = [escape_identifier(col) for col in source_columns]
        source_cols_str = ", ".join(escaped_source_cols)

        # Insert values from source with SCD2 metadata
        insert_source_vals = ", ".join([f"source.{escape_identifier(col)}" for col in source_columns])

        # Escaped names
        escaped_target = escape_qualified_name(schema, target_table)
        escaped_source = escape_identifier(source_table)
        escaped_valid_from = escape_identifier(valid_from_col)
        escaped_valid_to = escape_identifier(valid_to_col)
        escaped_is_current = escape_identifier(is_current_col)
        escaped_hash = escape_identifier(hash_col)

        # Build the SCD Type 2 SQL
        # This uses a multi-statement approach for clarity and DuckDB compatibility

        sql_statements = []

        # Statement 1: Expire changed records (set valid_to and is_current=FALSE)
        # Only expire records where the hash has changed
        expire_sql = f"""
UPDATE {escaped_target} AS target
SET {escaped_valid_to} = CURRENT_TIMESTAMP,
    {escaped_is_current} = FALSE
FROM {escaped_source} AS source
WHERE {on_conditions}
  AND target.{escaped_is_current} = TRUE
  AND target.{escaped_hash} != {hash_expr}"""
        sql_statements.append(expire_sql)

        # Statement 2: Insert new versions of changed records
        insert_changed_sql = f"""
INSERT INTO {escaped_target} ({source_cols_str}, {escaped_valid_from}, {escaped_valid_to}, {escaped_is_current}, {escaped_hash})
SELECT {insert_source_vals},
       CURRENT_TIMESTAMP,
       NULL,
       TRUE,
       {hash_expr}
FROM {escaped_source} AS source
WHERE EXISTS (
    SELECT 1 FROM {escaped_target} AS existing
    WHERE {on_conditions.replace('target.', 'existing.')}
      AND existing.{escaped_is_current} = FALSE
      AND existing.{escaped_valid_to} = (
          SELECT MAX(e2.{escaped_valid_to})
          FROM {escaped_target} e2
          WHERE {' AND '.join([f'e2.{escape_identifier(pk)} = source.{escape_identifier(pk)}' for pk in primary_key])}
      )
)"""
        sql_statements.append(insert_changed_sql)

        # Statement 3: Insert completely new records (no history exists)
        insert_new_sql = f"""
INSERT INTO {escaped_target} ({source_cols_str}, {escaped_valid_from}, {escaped_valid_to}, {escaped_is_current}, {escaped_hash})
SELECT {insert_source_vals},
       CURRENT_TIMESTAMP,
       NULL,
       TRUE,
       {hash_expr}
FROM {escaped_source} AS source
WHERE NOT EXISTS (
    SELECT 1 FROM {escaped_target} AS existing
    WHERE {on_conditions.replace('target.', 'existing.')}
)"""
        sql_statements.append(insert_new_sql)

        # Statement 4: Handle deletes (if delete_mode is not "ignore")
        if delete_mode in ("soft", "soft_mark"):
            # Close records that exist in target but not in source
            soft_delete_sql = f"""
UPDATE {escaped_target} AS target
SET {escaped_valid_to} = CURRENT_TIMESTAMP,
    {escaped_is_current} = FALSE"""

            if delete_mode == "soft_mark":
                soft_delete_sql += f",\n    {escape_identifier(deleted_col)} = TRUE"

            soft_delete_sql += f"""
WHERE target.{escaped_is_current} = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM {escaped_source} AS source
      WHERE {on_conditions}
  )"""
            sql_statements.append(soft_delete_sql)

        # Join statements with semicolons
        return ";\n".join(sql_statements)

    def _build_hash_expression(self, columns: List[str], table_alias: str) -> str:
        """
        Build MD5 hash expression for change detection.

        Args:
            columns: Columns to include in hash
            table_alias: Table alias for column references

        Returns:
            SQL expression for MD5 hash of columns
        """
        if not columns:
            return "''"  # Empty hash if no columns to track

        # Concatenate columns with delimiter, coalesce NULLs to empty string
        col_exprs = [
            f"COALESCE(CAST({table_alias}.{escape_identifier(col)} AS VARCHAR), '')"
            for col in columns
        ]
        concat_expr = " || '|' || ".join(col_exprs)

        # Use MD5 hash (supported by DuckDB and PostgreSQL)
        return f"MD5({concat_expr})"

    def needs_temp_table(self) -> bool:
        """SCD Type 2 strategy needs temp table for SQL generation."""
        return True

    def get_required_columns(self, scd2_config: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Get the SCD2 metadata columns that will be added to the table.

        Useful for schema management to know what columns the strategy adds.

        Args:
            scd2_config: Optional SCD2 configuration

        Returns:
            List of column names that SCD2 adds to the table
        """
        config = {**DEFAULT_SCD2_CONFIG, **(scd2_config or {})}

        columns = [
            config["valid_from_col"],
            config["valid_to_col"],
            config["is_current_col"],
            config["hash_col"],
        ]

        if config["delete_mode"] == "soft_mark":
            columns.append(config["deleted_col"])

        return columns

    def get_initial_insert_sql(
        self,
        connection: ibis.BaseBackend,
        target_table: str,
        schema: str,
        source_table: str,
        primary_key: str | List[str],
        scd2_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """
        Generate SQL for initial load (when target table is empty).

        For first load, all records are inserted as current versions.

        Args:
            connection: ibis connection backend
            target_table: Target dimension table name
            schema: Schema/database name
            source_table: Source table name
            primary_key: Business key column(s)
            scd2_config: SCD2 configuration

        Returns:
            SQL INSERT statement for initial load
        """
        if not primary_key:
            raise ValueError("primary_key required for scd_type_2 strategy")

        if isinstance(primary_key, str):
            primary_key = [primary_key]

        config = {**DEFAULT_SCD2_CONFIG, **(scd2_config or {})}
        tracked_columns = config["tracked_columns"]

        # Get source columns
        try:
            source = connection.table(source_table)
            source_schema = source.schema()
            all_columns = list(source_schema.keys())
        except Exception as e:
            raise ValueError(f"Cannot get schema for source table {source_table}: {e}")

        scd2_meta_cols = {
            config["valid_from_col"], config["valid_to_col"],
            config["is_current_col"], config["hash_col"], config["deleted_col"]
        }
        source_columns = [col for col in all_columns if col not in scd2_meta_cols]

        if tracked_columns is None:
            tracked_columns = [col for col in source_columns if col not in primary_key]

        hash_expr = self._build_hash_expression(tracked_columns, "source")

        escaped_target = escape_qualified_name(schema, target_table)
        escaped_source = escape_identifier(source_table)

        source_cols_str = ", ".join([escape_identifier(col) for col in source_columns])
        source_vals = ", ".join([f"source.{escape_identifier(col)}" for col in source_columns])

        scd2_cols = f"{escape_identifier(config['valid_from_col'])}, {escape_identifier(config['valid_to_col'])}, {escape_identifier(config['is_current_col'])}, {escape_identifier(config['hash_col'])}"

        return f"""
INSERT INTO {escaped_target} ({source_cols_str}, {scd2_cols})
SELECT {source_vals},
       CURRENT_TIMESTAMP,
       NULL,
       TRUE,
       {hash_expr}
FROM {escaped_source} AS source"""
