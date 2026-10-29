import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pipeline import generate_events,ingest,read_chunks
from benchmark import benchmark

def test_conflicting_replay_detected_and_existing_data_preserved(tmp_path):
    a=tmp_path/'events.csv';db=tmp_path/'data.db';generate_events(a,13)
    first=ingest(a,db,chunk_size=7)
    assert first['new_rows']==13 and first['replayed_rows']==0
    second=ingest(a,db,chunk_size=7)
    assert second['new_rows']==0 and second['replayed_rows']==13
    # Alter one event in an otherwise identical replay; this must fail.
    lines=a.read_text().splitlines(); fields=lines[1].split(',');fields[2]='changed';lines[1]=','.join(fields)
    a.write_text('\n'.join(lines)+'\n')
    with pytest.raises(ValueError,match='Conflicting replay'):
        ingest(a,db,chunk_size=7)
    clean=tmp_path/'clean.csv';generate_events(clean,13)
    assert ingest(clean,db,chunk_size=7)['stored_unique_rows']==13

def test_malformed_and_rejected_schema(tmp_path):
    data=tmp_path/'x.csv';db=tmp_path/'d.sqlite'
    for row in ['1,2,food,-1,2026-10-01','1,2,food,1,2026-99-99','0,2,food,1,2026-10-01',
                '1,2,,1,2026-10-01','1,2,food,1,2026-10-01,EXTRA']:
        data.write_text('event_id,user_id,category,amount_pence,event_day\n'+row+'\n')
        with pytest.raises(ValueError):ingest(data,db)

def test_multiscale_benchmark_is_measured(tmp_path):
    report=benchmark(sizes=[10,20],repeats=2,chunk_size=7)
    assert len(report['individual_runs'])==4
    assert [x['rows'] for x in report['summary']]==[10,20]
    assert all(x['median_ingest_s']>=0 for x in report['summary'])
