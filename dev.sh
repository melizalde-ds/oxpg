# scripts/dev.sh
#!/bin/bash
set -e  # Exit on error

echo "Generating stubs..."
cargo run --bin stub_gen

echo "Building with maturin..."
maturin develop

echo "✓ Build complete!"