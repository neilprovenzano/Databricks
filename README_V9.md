# V9 — competition-default fast paths (experimental)

Derived from the V8 source that scored 1.06s officially.

Changes:
- `READ_MMAP` defaults to **enabled** (V8 default was disabled). Disable with `READ_MMAP=0`.
- The mmap decision is cached once per process rather than querying the environment for each input chunk.
- `LINK_CACHE` defaults to **enabled** (V8 default was disabled). Disable with `LINK_CACHE=0`.
- `SCAN_SWAR` defaults to **enabled** as a fallback where AVX2 is unavailable. Disable with `SCAN_SWAR=0`.
- Runtime AVX2 detection from V8 is retained, default enabled on supported x86-64 CPUs.

These are performance *hypotheses*, not measured wins. In prior Mac paired tests mmap and SWAR improved throughput, but the official Linux environment could behave differently. Link caching also has not been shown to improve the official scorer.

Build: `cargo build --release --locked && cargo test --release --locked`

For a controlled comparison run the official scorer with V8 and V9, and compare official times. Keep V3 selected until V9 wins and passes all correctness tests.

This build has not been compiled or run in the assistant environment.
