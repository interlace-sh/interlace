#!/bin/bash
# Build the UI and copy to the static directory
# Run this before packaging interlace

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
UI_DIR="$ROOT_DIR/../interlace.ui"
STATIC_DIR="$ROOT_DIR/src/interlace/service/static"

echo "Building UI..."
echo "  Source: $UI_DIR"
echo "  Destination: $STATIC_DIR"

# Check UI directory exists
if [ ! -f "$UI_DIR/package.json" ]; then
    echo "Error: UI source not found at $UI_DIR"
    echo "  Expected sibling directory: ../interlace.ui/"
    exit 1
fi

# Install dependencies and build
cd "$UI_DIR"
pnpm install
pnpm build

# Clean and copy
rm -rf "$STATIC_DIR"
mkdir -p "$STATIC_DIR"
cp -r "$UI_DIR/dist/"* "$STATIC_DIR/"

echo "Done. UI built and copied to $STATIC_DIR"
ls -la "$STATIC_DIR"
