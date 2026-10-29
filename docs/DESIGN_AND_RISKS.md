# Design review and research roadmap

## Research questions

- Does ingestion remain deterministic, idempotent and correct under retries and malformed inputs?
- How do observed wall-clock time and Python-managed peak allocations change with dataset size?
- Do independent SQL engines produce the same aggregates on the same data?

## Known limitations

- SQLite uses a single-file database; no partitioning, external queue, fault-tolerant stream consumer, or distributed computation.
- Python `csv.DictReader` + SQLite writes are simple and transparent, not optimized for ultimate throughput.
- Per-batch atomicity; full-file rollback requires an explicit outer transaction.
- This demonstration uses synthetic records with limited variability and no distribution shift.
- Comparison to DuckDB is an aggregate correctness check, **not** a validated performance race: CSV scan and indexed SQLite table are different workloads.
- Native allocations / true peak RSS are not captured by tracemalloc.

## Evidence roadmap

1. Run benchmark with pinned machine spec and fresh database, keep complete raw JSON and command in a release.
2. Add Parquet input and DuckDB ingestion paths with common schema, equivalent validation and fair benchmark methodology.
3. Add property-based tests and schema-change/migration exercises.
4. Study consumer replays with queue-based semantics and failure injection.
5. Only then consider PySpark/distributed execution and justify the additional complexity with actual dataset sizes.

## Privacy and attribution

This is an original synthetic-data learning repository assisted by an AI coding tool. It does not reproduce employer source, private datasets, or credentials. All performance claims must refer to a specific committed version and environment.
