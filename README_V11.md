# V11 memory-path experiment

V11 tests two architectural hot spots without changing output semantics:

* `SCAT_DIRECT=0` (default): existing software write-combining intermediate-record scatter; `SCAT_DIRECT=1`: bypass staging buffers and append directly to per-bucket blocks across all stages.
* `DEDUP_CAP=1|2|3`: dedup hash table target capacity ~1.25x, 1.5x (default), or 2x the bucket record count, rounded to power of two.
* `DEDUP_PREFETCH=0|1` (default 1): toggle dedup hash-table prefetch.
* `DEDUP_SORT=0` retains winning hash dedup.

These are experimental performance switches; they do not guarantee a speedup. Test on both Mac and Linux, and validate output before scoring.
