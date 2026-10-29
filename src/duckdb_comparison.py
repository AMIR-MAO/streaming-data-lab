"""Optional DuckDB analytical query of the SAME generated events as SQLite.

Install `pip install duckdb` separately. Explicitly not required to run core tests.
Not a fair engine-speed comparison unless storage/configuration are controlled.
"""
import argparse
import csv
import sqlite3
from pathlib import Path

def totals_sqlite(db):
    with sqlite3.connect(db) as con:
        return con.execute('SELECT category,COUNT(*),SUM(amount_pence) FROM events GROUP BY category ORDER BY category').fetchall()

def totals_duckdb(csv_file):
    try: import duckdb
    except ImportError as e: raise RuntimeError('Install optional dependency: python -m pip install duckdb') from e
    con=duckdb.connect(':memory:')
    try:
        # CSV file path is a parameter; no SQL concatenation of untrusted paths.
        rows=con.execute("""SELECT category,COUNT(*),SUM(CAST(amount_pence AS BIGINT))
            FROM read_csv(?, header=true, all_varchar=true) GROUP BY category ORDER BY category""",[str(csv_file)]).fetchall()
        return rows
    finally: con.close()

def main():
    ap=argparse.ArgumentParser();ap.add_argument('csv',type=Path);ap.add_argument('sqlite',type=Path);a=ap.parse_args()
    sql=totals_sqlite(a.sqlite);duck=totals_duckdb(a.csv)
    if sql != duck: raise SystemExit(f'Mismatch: SQLite {sql} vs DuckDB {duck}')
    print('PASSED: SQLite and DuckDB category counts and integer-pence sums match')
if __name__=='__main__':main()
