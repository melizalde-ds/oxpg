#!/bin/bash
set -e  # Exit on error

echo "Generating stubs..."
cargo run --bin stub_gen

echo "Building distributable wheels..."
maturin build --release --features extension-module

echo "✓ Wheels built in target/wheels/"
ls -lh target/wheels/

echo "Fixing imports in stubs..."
sed -i 's/builtins\.Error/Error/g; s/builtins\.DatabaseError/DatabaseError/g' python/oxpg/__init__.pyi

echo "Syncing uv dependencies..."
uv sync

echo "✓ Build complete!"