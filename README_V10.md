# V10: Experimental alternative deduplication architecture

Adds `DEDUP_SORT=1`: sorts lightweight pointers to event records by event ID, chooses the earliest ingest timestamp, and scatters surviving records. `DEDUP_SORT=0` (default) retains V9's original hash-table deduplication. This is a large algorithmic experiment, NOT a proven optimization. Sorting can be slower than hashing.

Build: `cargo build --release --locked && cargo test --release --locked`

Benchmark both modes on the same warm dataset. Validate both outputs with DuckDB EXCEPT ALL and edge-case fixtures. Do not submit until Linux scoring confirms an improvement.

This environment does not have Rust installed, so compilation and correctness have not been verified here.
