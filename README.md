# Interlace Package

This is the core Interlace package, published to PyPI.

## Structure

```
pkg/
├── src/
│   └── interlace/          # Package source code
│       ├── __init__.py
│       ├── core/           # Core engine
│       ├── cli/            # CLI commands
│       ├── config/         # Configuration management
│       ├── strategies/     # Data loading strategies
│       ├── materialization/# Materialization
│       ├── connections/    # Connection management
│       └── utils/          # Utilities
├── tests/                  # Package tests
├── pyproject.toml         # Package configuration
└── README.md              # This file
```

## Development

```bash
# Install in development mode
uv pip install -e .

# Run tests
pytest tests/

# Build package
uv build

# Publish to PyPI
uv publish
```

## Using as Workspace Member

This package is part of the Interlace monorepo workspace. See the root `README.md` for workspace-level commands.
