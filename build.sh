#!/bin/bash
set -e  # Exit on error

echo "Generating stubs..."
cargo run --bin stub_gen

echo "Building distributable wheels..."
maturin build --release

echo "✓ Wheels built in target/wheels/"
ls -lh target/wheels/

echo "Syncing uv dependencies..."
uv sync

echo "✓ Build complete!"