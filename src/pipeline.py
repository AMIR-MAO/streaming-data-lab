"""Synthetic event generation with validated bounded-batch ingestion into SQLite.

Standard library only. No user data. Repeat ingestion is idempotent; conflicting
replays fail loudly rather than silently masking changed transactions.
"""
from __future__ import annotations
import argparse
import csv
import json
import random
import sqlite3
import time
from datetime import date
from pathlib import Path

COLUMNS = ('event_id', 'user_id', 'category', 'amount_pence', 'event_day')
SCHEMA = """CREATE TABLE IF NOT EXISTS events (
 event_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL CHECK(user_id>0),
 category TEXT NOT NULL CHECK(length(category)>0),
 amount_pence INTEGER NOT NULL CHECK(amount_pence>=0),
 event_day TEXT NOT NULL
);"""

def generate_events(path, rows=100000, seed=42):
    if rows < 0:
        raise ValueError('rows must be nonnegative')
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f); writer.writerow(COLUMNS)
        for i in range(1, rows+1):
            writer.writerow([i, rng.randint(1,10000), rng.choice(('food','travel','utilities','shopping')),
                rng.randint(1,25000), f'2026-10-{rng.randint(1,28):02}'])

def _parse(row, line):
    try:
        if None in row or not all(col in row for col in COLUMNS):
            raise ValueError('incorrect column count')
        event_id=int(row['event_id']); user_id=int(row['user_id'])
        category=row['category'].strip(); amount=int(row['amount_pence'])
        day=row['event_day']; parsed=date.fromisoformat(day)
        if event_id <= 0 or user_id <= 0 or amount < 0 or not category or parsed.isoformat()!=day:
            raise ValueError('constraint violation')
        return event_id, user_id, category, amount, day
    except (ValueError, TypeError) as e:
        raise ValueError(f'Invalid event at CSV line {line}: {e}') from e

def read_chunks(path, chunk_size=5000):
    if chunk_size < 1: raise ValueError('chunk_size must be >= 1')
    with Path(path).open(newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames is None or tuple(reader.fieldnames)!=COLUMNS:
            raise ValueError(f'Expected columns: {COLUMNS}')
        chunk=[]
        for line,row in enumerate(reader,start=2):
            chunk.append(_parse(row,line))
            if len(chunk)==chunk_size:
                yield chunk;chunk=[]
        if chunk: yield chunk

def ingest(csv_path, database_path, chunk_size=5000):
    if chunk_size < 1: raise ValueError('chunk_size must be >= 1')
    database_path=Path(database_path)
    database_path.parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(database_path)
    con.execute(SCHEMA)
    processed=0; inserted=0; replayed=0
    try:
        for batch in read_chunks(csv_path,chunk_size):
            # Atomic per-batch write: duplicate keys are accepted only if all fields agree.
            with con:
                for rec in batch:
                    cursor=con.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)',rec)
                    if cursor.rowcount:
                        inserted+=1
                    else:
                        old=con.execute('SELECT event_id,user_id,category,amount_pence,event_day FROM events WHERE event_id=?',(rec[0],)).fetchone()
                        if old != rec:
                            raise ValueError(f'Conflicting replay/constraint failure for event_id={rec[0]}')
                        replayed+=1
            processed+=len(batch)
        count=con.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        agg=con.execute('SELECT category, COUNT(*), SUM(amount_pence) FROM events GROUP BY category ORDER BY category').fetchall()
        return {'input_rows':processed, 'new_rows':inserted, 'replayed_rows':replayed,
            'stored_unique_rows':count,
            'totals_by_category':[{'category':a,'rows':b,'amount_pence':c} for a,b,c in agg]}
    finally:
        con.close()

def main():
    p=argparse.ArgumentParser(); p.add_argument('--rows',type=int,default=100000)
    p.add_argument('--chunk-size',type=int,default=5000)
    p.add_argument('--dir',default='output'); a=p.parse_args()
    if a.rows<0 or a.chunk_size<1: p.error('rows must be >=0 and chunk-size >=1')
    out=Path(a.dir);out.mkdir(parents=True,exist_ok=True)
    csv_path=out/'events.csv';db=out/'events.sqlite'
    start=time.perf_counter();generate_events(csv_path,a.rows);gen=time.perf_counter()-start
    start=time.perf_counter();r=ingest(csv_path,db,a.chunk_size);elapsed=time.perf_counter()-start
    r.update({'generated_rows':a.rows,'generation_seconds':round(gen,4),
        'ingest_seconds':round(elapsed,4),'ingest_rows_per_second':round(a.rows/max(elapsed,1e-12)),
        'chunk_size':a.chunk_size,'data':'synthetic'})
    (out/'report.json').write_text(json.dumps(r,indent=2)+'\n')
    print(json.dumps(r,indent=2))
if __name__=='__main__': main()
