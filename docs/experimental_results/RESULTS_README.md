# Local data engineering benchmarks

Synthetic event records only. The 100,000-row single run used a **fresh temporary database**, a 5,000-row application-side batch size, and records the timings in `sqlite_single_run_2026-10-27.json`. Performance is environment-specific.

The separate multi-scale runs use sizes 1,000 / 10,000 / 30,000, 2 repeats each, fresh databases, and report medians and Python-managed peak allocations in `sqlite_multiscale_2026-10-27.json`. This is small-scale benchmark methodology and NOT evidence of distributed processing.

To repeat: `python src/benchmark.py --sizes 1000 10000 30000 --repeats 2 --output output/benchmark.json`.
