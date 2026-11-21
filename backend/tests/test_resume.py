"""Resumable-run behaviour: checkpoint resume, fencing and single-run parity.

Run with a PostgreSQL 16 database, e.g.:

    GB5311_TEST_DATABASE_URL=postgresql+psycopg://app:secret@127.0.0.1:5432/workflow \
      python -m pytest -q backend/tests/test_resume.py
"""

import os

import pytest
from sqlalchemy import create_engine, select

from src.constants.workflow import Status
from src.models.tables import evidence, migrate, run_ledger, run_stock, run_trail, runs, schema
from src.services import workflow
from src.services.domain import SPEC, solve
from src.services.run_stock import Fenced, Store

URL = os.environ.get('GB5311_TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not URL, reason='GB5311_TEST_DATABASE_URL 未设置')

CONFLICTING = {'batch_id': 'BL-401', 'revision': 1, 'action': '记账', 'grade': 'B级',
               'weight_g': 8100, 'pieces': 5}


@pytest.fixture()
def engine():
    eng = create_engine(URL)
    schema.drop_all(eng)
    schema.create_all(eng)
    migrate(eng)
    workflow.engine = eng  # the service module keeps its own engine reference
    yield eng
    workflow.engine = None
    eng.dispose()


def start_run(engine, payload):
    case = workflow.make_case('断点续跑验证', payload, 'tester')
    run = workflow.enqueue(case['id'], 'tester')
    run_id, token, claimed = workflow.claim()
    assert (run_id, claimed) == (run['id'], case['payload'])
    return run_id, token


def crash_after(engine, run_id, token, payload, entries):
    """Post the opening stock and `entries` revisions, then stop like a crashed worker."""
    store = Store(engine, run_id, token)
    assert store.initialize(payload['opening_batches']) == 0
    for index, event in enumerate(payload['revisions'], 1):
        if index > entries:
            break
        assert store.apply(index, event)['status'] != '版本冲突'


def reclaim(engine, run_id):
    """Force the stale-claim path and return the new attempt token."""
    with engine.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == run_id)
                     .values(started_at='2000-01-01T00:00:00+00:00'))
    reclaimed_id, token, _ = workflow.claim()
    assert reclaimed_id == run_id
    return token


def processed_of(engine, run_id):
    with engine.connect() as conn:
        return conn.execute(select(runs.c.processed).where(runs.c.id == run_id)).scalar_one()


def test_resumable_matches_single_run(engine):
    expected = solve(SPEC['sample'], engine)
    run_id, token = start_run(engine, SPEC['sample'])
    result = solve(SPEC['sample'], engine, run_id=run_id, token=token)
    assert result == expected
    assert all(result['metrics'][key] == value for key, value in SPEC['expect'].items())
    workflow.finish(run_id, token, result=result)
    assert processed_of(engine, run_id) == len(SPEC['sample']['revisions'])
    with engine.connect() as conn:
        for table in (run_ledger, run_stock, run_trail):
            assert conn.execute(select(table)).first() is None  # checkpoints cleaned up


@pytest.mark.parametrize('stop', [0, 1, 3, 5, 6])
def test_crash_resume_matches_single_run(engine, stop):
    expected = solve(SPEC['sample'], engine)
    run_id, token = start_run(engine, SPEC['sample'])
    crash_after(engine, run_id, token, SPEC['sample'], stop)
    assert processed_of(engine, run_id) == stop

    new_token = reclaim(engine, run_id)
    result = solve(SPEC['sample'], engine, run_id=run_id, token=new_token)
    assert result == expected  # 分级重量、件数与轨迹与一次跑完相同
    workflow.finish(run_id, new_token, result=result)

    with engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
        assert run['status'] == Status.COMPLETE and run['processed'] == len(SPEC['sample']['revisions'])
        saved = conn.execute(select(evidence.c.report).where(evidence.c.run_id == run_id)).all()
    assert len(saved) == 1 and saved[0][0] == expected  # 对账通过后只有一份配额单


def test_superseded_attempt_cannot_write_or_finish(engine):
    run_id, old_token = start_run(engine, SPEC['sample'])
    crash_after(engine, run_id, old_token, SPEC['sample'], 2)
    new_token = reclaim(engine, run_id)

    stale = Store(engine, run_id, old_token)
    with pytest.raises(Fenced):
        stale.apply(3, SPEC['sample']['revisions'][2])
    workflow.finish(run_id, old_token, result={'summary': '旧进程补发'})  # silently dropped

    result = solve(SPEC['sample'], engine, run_id=run_id, token=new_token)
    workflow.finish(run_id, new_token, result=result)
    with engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
        reports = conn.execute(select(evidence.c.report).where(evidence.c.run_id == run_id)).scalars().all()
    assert run['status'] == Status.COMPLETE
    assert reports == [solve(SPEC['sample'], engine)]  # 旧进程没有发单


def test_requeue_resumes_same_run(engine):
    run_id, token = start_run(engine, SPEC['sample'])
    crash_after(engine, run_id, token, SPEC['sample'], 3)
    workflow.requeue(run_id, token)
    with engine.connect() as conn:
        assert conn.execute(select(runs.c.status).where(runs.c.id == run_id)).scalar_one() == Status.QUEUED

    new_id, new_token, _ = workflow.claim()
    assert new_id == run_id  # 留在同一次运行里
    result = solve(SPEC['sample'], engine, run_id=run_id, token=new_token)
    assert result == solve(SPEC['sample'], engine)
    workflow.finish(run_id, new_token, result=result)


def test_first_conflict_identical_after_resume(engine):
    payload = {**SPEC['sample'], 'revisions': [*SPEC['sample']['revisions'], CONFLICTING]}
    expected = solve(payload, engine)
    assert expected['metrics']['首个冲突流水行'] == len(payload['revisions'])

    run_id, token = start_run(engine, payload)
    crash_after(engine, run_id, token, payload, 4)
    new_token = reclaim(engine, run_id)
    result = solve(payload, engine, run_id=run_id, token=new_token)
    assert result == expected  # 首个冲突与冲突前轨迹一致
    workflow.finish(run_id, new_token, result=result)
    with engine.connect() as conn:
        assert conn.execute(select(evidence).where(evidence.c.run_id == run_id)).first() is not None


def test_conflict_before_crash_is_recomputed(engine):
    altered = {'batch_id': 'BL-101', 'revision': 3, 'action': '记账', 'grade': 'B级',
               'weight_g': 21000, 'pieces': 13}
    payload = {**SPEC['sample'], 'revisions': [altered if i == 2 else row
                                               for i, row in enumerate(SPEC['sample']['revisions'])]}
    expected = solve(payload, engine)
    run_id, token = start_run(engine, payload)
    crash_after(engine, run_id, token, payload, 1)
    # The conflict sits at entry 3, after the crash point: found only on resume.
    new_token = reclaim(engine, run_id)
    result = solve(payload, engine, run_id=run_id, token=new_token)
    assert result == expected
    assert result['metrics']['首个冲突流水行'] == 3
