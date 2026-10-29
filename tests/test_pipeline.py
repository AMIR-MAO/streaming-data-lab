import sys, sqlite3
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pipeline import generate_events,ingest,read_chunks

def test_ingestion_and_idempotency(tmp_path):
    csv_path=tmp_path/'events.csv';db=tmp_path/'events.db'
    generate_events(csv_path,103)
    assert [len(c) for c in read_chunks(csv_path,25)]==[25,25,25,25,3]
    r=ingest(csv_path,db,25)
    assert r['stored_unique_rows']==103
    again=ingest(csv_path,db,25)
    assert again['stored_unique_rows']==103
    assert sum(x['rows'] for x in r['totals_by_category'])==103

def test_invalid_data(tmp_path):
    p=tmp_path/'bad.csv';p.write_text('event_id,user_id,category,amount_pence,event_day\n1,1,food,-2,2026-10-01\n')
    with pytest.raises(ValueError):ingest(p,tmp_path/'db.sqlite')
