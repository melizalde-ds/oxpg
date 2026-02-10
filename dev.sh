# scripts/dev.sh
#!/bin/bash
set -e  # Exit on error

echo "Generating stubs..."
cargo run --bin stub_gen

echo "Building with maturin..."
maturin develop

echo "Fixing imports in stubs..."
sed -i 's/builtins\.Error/Error/g; s/builtins\.DatabaseError/DatabaseError/g' python/oxpg/__init__.pyi

echo "Syncing uv dependencies..."
uv sync

echo "✓ Build complete!"