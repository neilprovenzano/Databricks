# Shopping Sessions — native Rust pipeline

Single static-ish Rust binary; `Dockerfile` builds it and runs it with no arguments.
Reads `$DATA_DIR`, writes Parquet to `$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/`.

Pipeline: parallel SIMD CSV scan → event_id-partitioned dedup → user-partitioned
linked-ID lookup (union-find built concurrently) → person-partitioned sessionization,
attribution and assists → Parquet.
