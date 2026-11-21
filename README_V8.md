# V8 experimental Linux x86-64 AVX2 runtime dispatch

Based on validated V7.1. On x86-64, AVX2 delimiter masks are now compiled in a separate target-feature function and enabled at runtime when the CPU supports AVX2. This avoids depending on global `RUSTFLAGS=-C target-feature=+avx2`.

- `SCAN_AVX2=1` (default): use AVX2 when CPU supports it.
- `SCAN_AVX2=0 SCAN_SWAR=1`: force the SWAR scanner for an apples-to-apples test.
- `SCAN_AVX2=0 SCAN_SWAR=0`: force scalar scanner.
- On ARM/macOS, AVX2 is unavailable; use `SCAN_SWAR=1`.

Build with `cargo build --release --locked` and `cargo test --release --locked`.

This patch has NOT been compiled or benchmarked in the assistant environment. Validate outputs against both official dev answers and all challenge edge fixtures before considering a submission. The official scorer's CPU, workload, compiler flags and measurement environment determine leaderboard performance. Do not replace the selected V3 until the scorer confirms a better result.
