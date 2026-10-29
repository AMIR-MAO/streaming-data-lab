# Streaming Data Lab | validated event ingestion & reproducible performance study

A standalone Python/SQLite **data engineering prototype** covering deterministic synthetic data generation, validated chunked ingestion, idempotent replay, explicit conflict detection, SQL aggregations, and multi-scale performance measurement. Includes an **optional** DuckDB analytical cross-check. It is **not** a distributed Big Data system.

## Why this matters

In real data engineering, correct results and reproducible measurements matter at least as much as raw throughput. This lab tests data integrity constraints, controlled replays, and how input volume changes processing time and Python-managed peak allocations.

## Features

- Deterministic generation of synthetic event records with integer-pence values (no customer data)
- Bounded application-side CSV chunks, validation of IDs, ISO dates, categories and nonnegative amounts
- SQLite constraints and explicit conflict detection if an event ID is replayed with different contents
- Category-level aggregation using SQL, alongside exact integer values
- Benchmark: fresh SQLite database per run, configurable dataset sizes/repeats, medians and Python-traced allocation peaks
- Optional query reconciliation against DuckDB CSV scans (needs an extra installation)

## Run

```bash
python -m pip install -r requirements.txt
python -m pytest -q tests
python src/pipeline.py --rows 100000 --chunk-size 5000 --dir output
python src/benchmark.py --sizes 1000 10000 100000 --repeats 3 --output output/benchmark.json
```

Optional cross-engine correctness check:

```bash
python -m pip install duckdb
python src/duckdb_comparison.py output/events.csv output/events.sqlite
```

These commands do **not** require Docker, paid APIs, cloud infrastructure, or previous employers' datasets. `output/` is gitignored.

## Architecture

```text
Deterministic synthetic CSV
         |
         v
   CSV DictReader ---> strict typed parser + field validation
         |
         v
      bounded batches
         |
         v
 SQLite INSERT OR IGNORE --> on duplicate: compare all fields; mismatch = failure
         |
         v
  grouped SQL aggregates --> JSON report
         |
         +-- benchmark runner (fresh DB per replicate, Python allocations)
         +-- optional DuckDB CSV query (aggregate correctness comparison)
```

## Correctness contract

1. Existing rows must remain unchanged after exact replay; conflicts raise errors.
2. Input IDs and user IDs must be positive; category non-empty; pence nonnegative; date valid ISO-8601.
3. Per-batch writes are atomic; earlier successfully committed chunks can remain if a later chunk fails. **This is not a whole-file transaction.**
4. SQLite `INSERT OR IGNORE` is checked by comparing the full stored row when no insert occurs, so duplicate IDs cannot silently change existing values.
5. This pipeline does not guarantee exactly-once semantics across external messaging systems.

## Benchmark validity

- The benchmark uses a fresh local SQLite DB for each run and excludes synthetic CSV generation from timed ingestion.
- `tracemalloc` measures Python-managed allocations only, **not full RSS or native SQLite memory**.
- Reported performance depends on hardware, Python/SQLite versions, filesystem and runtime settings.
- Results are not measurements of Apache Spark or an actual Big Data cluster.

See `docs/DESIGN_AND_RISKS.md` for fuller assessment and honest next milestones.
