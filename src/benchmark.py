"""Fresh-database multi-scale benchmark with explicit limitations.

Peak memory measures Python-traced allocations only, not total process RSS.
"""
import argparse
import csv
import json
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from pipeline import generate_events, ingest

def benchmark(sizes=(1000,10000,100000), repeats=3, chunk_size=5000):
    if repeats<1 or any(n<0 for n in sizes) or chunk_size<1: raise ValueError('invalid args')
    rows=[]
    with tempfile.TemporaryDirectory() as root:
        folder=Path(root)
        for n in sizes:
            for rep in range(repeats):
                csv_path=folder/f'{n}_{rep}.csv';db_path=folder/f'{n}_{rep}.db'
                generate_events(csv_path,n,seed=42+rep)
                tracemalloc.start();start=time.perf_counter()
                report=ingest(csv_path,db_path,chunk_size)
                elapsed=time.perf_counter()-start;_,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
                assert report['stored_unique_rows']==n
                rows.append({'rows':n,'repeat':rep,'ingest_s':round(elapsed,5),
                    'rows_per_s':round(n/max(elapsed,1e-12)),'traced_python_peak_mib':round(peak/1024**2,3)})
    summary=[]
    for n in sizes:
        q=[r for r in rows if r['rows']==n]
        summary.append({'rows':n, 'repeats':repeats,
            'median_ingest_s':round(statistics.median(r['ingest_s'] for r in q),5),
            'median_rows_per_s':round(statistics.median(r['rows_per_s'] for r in q)),
            'max_python_traced_peak_mib':max(r['traced_python_peak_mib'] for r in q)})
    return {'platform':platform.platform(),'python':sys.version.split()[0],
        'backend':'SQLite local', 'chunk_size':chunk_size,
        'memory_note':'tracemalloc Python allocations only, excludes native SQLite/cache and OS RSS',
        'measurement_note':'fresh databases; excludes CSV generation and includes SQLite creation',
        'individual_runs':rows,'summary':summary}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--sizes',type=int,nargs='+',default=[1000,10000,100000]);
    ap.add_argument('--repeats',type=int,default=3);ap.add_argument('--chunk-size',type=int,default=5000)
    ap.add_argument('--output',type=Path,default=Path('output/benchmark.json'));a=ap.parse_args()
    report=benchmark(a.sizes,a.repeats,a.chunk_size);a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report['summary'],indent=2))
if __name__=='__main__':main()
