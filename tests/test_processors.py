"""
Tests for the processor registry module.
"""

import pytest

from interlace.sync.types import FileProcessor, ProcessorSpec


@pytest.fixture
def registry() -> dict[str, FileProcessor]:
    """Build registry, importing lazily to avoid circular import."""
    from interlace.processors.registry import build_default_processor_registry

    return build_default_processor_registry()


def _resolve(specs: list[ProcessorSpec] | None, registry: dict[str, FileProcessor]) -> list:
    from interlace.processors.registry import resolve_processors

    return resolve_processors(specs, registry=registry)


class TestBuildDefaultRegistry:
    def test_returns_dict(self, registry: dict) -> None:
        assert isinstance(registry, dict)

    def test_pgp_registered_when_available(self, registry: dict) -> None:
        # pgpy is a project dependency, so PGP processor should be available
        assert "pgp_decrypt" in registry


class TestResolveProcessors:
    def test_empty_specs(self, registry: dict) -> None:
        assert _resolve(None, registry) == []
        assert _resolve([], registry) == []

    def test_valid_spec(self, registry: dict) -> None:
        specs = [ProcessorSpec(name="pgp_decrypt", config={"key_path": "/tmp/key.asc"})]
        result = _resolve(specs, registry)
        assert len(result) == 1
        proc, config = result[0]
        assert proc.name == "pgp_decrypt"
        assert config["key_path"] == "/tmp/key.asc"

    def test_unknown_processor_raises(self, registry: dict) -> None:
        specs = [ProcessorSpec(name="nonexistent")]
        with pytest.raises(ValueError, match="Unknown processor 'nonexistent'"):
            _resolve(specs, registry)

    def test_preserves_order(self, registry: dict) -> None:
        class FakeProcessor:
            name = "fake"

            def process(self, path: str, metadata: dict, config: dict) -> str:
                return path

        registry["fake"] = FakeProcessor()  # type: ignore[assignment]
        specs = [
            ProcessorSpec(name="fake", config={}),
            ProcessorSpec(name="pgp_decrypt", config={"key_path": "/k"}),
        ]
        result = _resolve(specs, registry)
        assert result[0][0].name == "fake"
        assert result[1][0].name == "pgp_decrypt"

    def test_none_config_becomes_empty_dict(self, registry: dict) -> None:
        specs = [ProcessorSpec(name="pgp_decrypt")]
        result = _resolve(specs, registry)
        _, config = result[0]
        assert config == {}
