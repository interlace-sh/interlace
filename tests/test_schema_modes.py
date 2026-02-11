"""
Tests for schema modes and flexible schema handling.

Tests for SchemaMode enum, DataConverter with fields/strict, and schema validation.
"""

import pytest
import ibis
from interlace.schema.modes import SchemaMode
from interlace.schema.validation import (
    validate_schema,
    compare_schemas,
    SchemaComparisonResult,
)


class TestSchemaMode:
    """Tests for SchemaMode enum."""

    def test_schema_mode_values(self):
        """Test SchemaMode enum has expected values."""
        assert SchemaMode.STRICT.value == "strict"
        assert SchemaMode.SAFE.value == "safe"
        assert SchemaMode.FLEXIBLE.value == "flexible"
        assert SchemaMode.LENIENT.value == "lenient"
        assert SchemaMode.IGNORE.value == "ignore"

    def test_schema_mode_from_string(self):
        """Test creating SchemaMode from string."""
        assert SchemaMode("strict") == SchemaMode.STRICT
        assert SchemaMode("safe") == SchemaMode.SAFE
        assert SchemaMode("flexible") == SchemaMode.FLEXIBLE
        assert SchemaMode("lenient") == SchemaMode.LENIENT
        assert SchemaMode("ignore") == SchemaMode.IGNORE

    def test_schema_mode_invalid_string(self):
        """Test invalid string raises ValueError."""
        with pytest.raises(ValueError):
            SchemaMode("invalid")


class TestSchemaComparisonResult:
    """Tests for SchemaComparisonResult dataclass."""

    def test_compatible_result(self):
        """Test creating a compatible result."""
        result = SchemaComparisonResult(
            added_columns={},
            removed_columns=[],
            type_changes=[],
            compatible=True,
            warnings=[],
            errors=[],
        )
        assert result.compatible is True
        assert len(result.errors) == 0

    def test_incompatible_result(self):
        """Test creating an incompatible result."""
        result = SchemaComparisonResult(
            added_columns={},
            removed_columns=[],
            type_changes=[("id", "int64", "string")],
            compatible=False,
            warnings=[],
            errors=["Unsafe type change"],
        )
        assert result.compatible is False
        assert len(result.errors) == 1


class TestCompareSchemas:
    """Tests for compare_schemas function."""

    @pytest.fixture
    def existing_schema(self):
        """Create a sample existing schema."""
        return ibis.schema([
            ("id", "int64"),
            ("name", "string"),
            ("value", "float64"),
        ])

    def test_identical_schemas(self, existing_schema):
        """Test comparing identical schemas."""
        result = compare_schemas(existing_schema, existing_schema)

        assert result.compatible is True
        assert len(result.added_columns) == 0
        assert len(result.removed_columns) == 0
        assert len(result.type_changes) == 0

    def test_detect_added_columns(self, existing_schema):
        """Test detecting added columns."""
        new_schema = ibis.schema([
            ("id", "int64"),
            ("name", "string"),
            ("value", "float64"),
            ("new_col", "string"),
        ])

        result = compare_schemas(existing_schema, new_schema)

        assert "new_col" in result.added_columns
        assert result.compatible is True

    def test_detect_removed_columns(self, existing_schema):
        """Test detecting removed columns."""
        new_schema = ibis.schema([
            ("id", "int64"),
            ("name", "string"),
        ])

        result = compare_schemas(existing_schema, new_schema)

        assert "value" in result.removed_columns
        assert result.compatible is True

    def test_detect_type_changes(self, existing_schema):
        """Test detecting type changes."""
        new_schema = ibis.schema([
            ("id", "string"),  # Changed from int64
            ("name", "string"),
            ("value", "float64"),
        ])

        result = compare_schemas(existing_schema, new_schema)

        assert len(result.type_changes) == 1
        assert result.type_changes[0][0] == "id"
        # Unsafe type change should make it incompatible
        assert result.compatible is False

    def test_safe_type_widening(self):
        """Test safe type widening is compatible."""
        existing = ibis.schema([("id", "int32")])
        new = ibis.schema([("id", "int64")])

        result = compare_schemas(existing, new)

        assert result.compatible is True

    def test_with_fields_schema(self, existing_schema):
        """Test comparison with explicit fields schema."""
        new_schema = ibis.schema([
            ("id", "int64"),
            ("name", "string"),
            ("value", "float64"),
            ("extra", "string"),  # Extra column
        ])
        # Only specify the fields we care about
        fields_schema = ibis.schema([
            ("id", "int64"),
            ("name", "string"),
        ])

        result = compare_schemas(existing_schema, new_schema, fields_schema)

        # Should not report "extra" or "value" as additions/removals
        # because we only care about specified fields
        assert result.compatible is True


class TestValidateSchema:
    """Tests for validate_schema function."""

    def test_no_existing_schema(self):
        """Test validation with no existing schema (new table)."""
        new_schema = ibis.schema([("id", "int64")])

        result = validate_schema(None, new_schema)

        assert result.compatible is True
        assert len(result.added_columns) == 0
        assert len(result.errors) == 0

    def test_with_existing_schema(self):
        """Test validation with existing schema."""
        existing = ibis.schema([("id", "int64")])
        new = ibis.schema([("id", "int64"), ("name", "string")])

        result = validate_schema(existing, new)

        assert result.compatible is True
        assert "name" in result.added_columns


class TestDataConverterColumnMapping:
    """Tests for DataConverter with column mapping (renames)."""

    def test_apply_column_mapping_rename(self):
        """Test applying column rename mapping."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"old_name": "value1", "other": "value2"}]
        mapping = {"old_name": "new_name"}

        result = DataConverter.apply_column_mapping(data, mapping)

        assert len(result) == 1
        assert "new_name" in result[0]
        assert "old_name" not in result[0]
        assert result[0]["new_name"] == "value1"
        assert result[0]["other"] == "value2"

    def test_apply_column_mapping_multiple(self):
        """Test applying multiple column renames."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"a": 1, "b": 2, "c": 3}]
        mapping = {"a": "x", "b": "y"}

        result = DataConverter.apply_column_mapping(data, mapping)

        assert result[0]["x"] == 1
        assert result[0]["y"] == 2
        assert result[0]["c"] == 3
        assert "a" not in result[0]
        assert "b" not in result[0]

    def test_apply_column_mapping_empty_data(self):
        """Test applying mapping to empty data."""
        from interlace.core.execution.data_converter import DataConverter

        data = []
        mapping = {"old": "new"}

        result = DataConverter.apply_column_mapping(data, mapping)

        assert result == []

    def test_apply_column_mapping_no_mapping(self):
        """Test applying empty mapping is no-op."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"a": 1, "b": 2}]
        mapping = {}

        result = DataConverter.apply_column_mapping(data, mapping)

        assert result == data

    def test_convert_to_ibis_table_with_mapping(self):
        """Test converting data with column mapping."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"old_col": 1, "value": 2}]
        mapping = {"old_col": "new_col"}

        table = DataConverter.convert_to_ibis_table(data, column_mapping=mapping)

        assert "new_col" in table.columns
        assert "old_col" not in table.columns
        assert "value" in table.columns


class TestDataConverterFields:
    """Tests for DataConverter with fields parameter."""

    def test_convert_with_fields_merge_mode(self):
        """Test converting with fields in merge mode (default)."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"id": 1, "name": "test", "extra": "value"}]
        fields = {"id": "int64", "name": "string"}

        table = DataConverter.convert_to_ibis_table(data, fields=fields, strict=False)

        # Should have all columns including extra
        assert "id" in table.columns
        assert "name" in table.columns
        assert "extra" in table.columns

    def test_convert_with_fields_strict_mode(self):
        """Test converting with fields in strict mode."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"id": 1, "name": "test", "extra": "value"}]
        fields = {"id": "int64", "name": "string"}

        table = DataConverter.convert_to_ibis_table(data, fields=fields, strict=True)

        # Should only have specified columns
        assert "id" in table.columns
        assert "name" in table.columns
        assert "extra" not in table.columns

    def test_convert_with_fields_type_enforcement(self):
        """Test that fields parameter enforces types."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"id": 1, "name": "test"}]
        fields = {"id": "string"}  # Coerce int to string

        table = DataConverter.convert_to_ibis_table(data, fields=fields)
        schema = table.schema()

        # The type should be as specified in fields
        assert "string" in str(schema["id"]).lower()

    def test_convert_no_fields(self):
        """Test converting without fields uses inferred schema."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"id": 1, "name": "test"}]

        table = DataConverter.convert_to_ibis_table(data)

        assert "id" in table.columns
        assert "name" in table.columns


class TestDataConverterCombined:
    """Tests for DataConverter with both mapping and fields."""

    def test_mapping_then_strict_fields(self):
        """Test applying mapping before strict fields."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"old_id": 1, "old_name": "test", "unwanted": "drop"}]
        mapping = {"old_id": "id", "old_name": "name"}
        fields = {"id": "int64", "name": "string"}

        table = DataConverter.convert_to_ibis_table(
            data,
            fields=fields,
            strict=True,
            column_mapping=mapping,
        )

        assert "id" in table.columns
        assert "name" in table.columns
        assert "old_id" not in table.columns
        assert "old_name" not in table.columns
        assert "unwanted" not in table.columns


class TestDataConverterSmartCoercion:
    """Tests for smart type coercion (unknown → JSON)."""

    def test_heterogeneous_array_becomes_json(self):
        """Test that heterogeneous arrays are coerced to JSON."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"id": 1, "mixed": [1, "two", 3.0]}]
        table = DataConverter.convert_to_ibis_table(data)

        assert "json" in str(table.schema()["mixed"]).lower()

    def test_normal_arrays_stay_typed(self):
        """Test that homogeneous arrays keep their proper types."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"int_arr": [1, 2, 3], "str_arr": ["a", "b"]}]
        table = DataConverter.convert_to_ibis_table(data)
        schema = table.schema()

        assert "array<int64>" in str(schema["int_arr"]).lower()
        assert "array<string>" in str(schema["str_arr"]).lower()

    def test_nested_structs_stay_typed(self):
        """Test that nested dicts become struct types."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"nested": {"a": 1, "b": "two"}}]
        table = DataConverter.convert_to_ibis_table(data)
        schema = table.schema()

        assert "struct" in str(schema["nested"]).lower()

    def test_mixed_normal_and_heterogeneous(self):
        """Test mix of normal and heterogeneous columns."""
        from interlace.core.execution.data_converter import DataConverter

        data = [{"normal": [1, 2, 3], "mixed": [1, "two", {"x": 3}]}]
        table = DataConverter.convert_to_ibis_table(data)
        schema = table.schema()

        assert "array<int64>" in str(schema["normal"]).lower()
        assert "json" in str(schema["mixed"]).lower()


class TestModelDecoratorSimplified:
    """Tests for simplified @model decorator parameters."""

    def test_model_with_fields_strict(self):
        """Test @model with fields and strict=True."""
        from interlace.core.model import model

        @model(
            name="test_model",
            fields={"id": int, "name": str},
            strict=True,
        )
        def test_model():
            return [{"id": 1, "name": "test", "extra": "dropped"}]

        metadata = test_model._interlace_model
        assert metadata["fields"] == {"id": int, "name": str}
        assert metadata["strict"] is True

    def test_model_with_column_mapping(self):
        """Test @model with column_mapping dict."""
        from interlace.core.model import model

        @model(
            name="test_model",
            column_mapping={"old": "new"},
        )
        def test_model():
            return [{"old": 1}]

        metadata = test_model._interlace_model
        assert metadata["column_mapping"] == {"old": "new"}

    def test_model_default_strict_false(self):
        """Test @model defaults to strict=False."""
        from interlace.core.model import model

        @model(name="test_model")
        def test_model():
            return [{"id": 1}]

        metadata = test_model._interlace_model
        assert metadata["strict"] is False
