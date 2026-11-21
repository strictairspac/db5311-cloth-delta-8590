from sqlalchemy import (JSON, BigInteger, Column, ForeignKey, Integer,
                        MetaData, PrimaryKeyConstraint, String, Table, Text,
                        text)

schema = MetaData()
users = Table('accounts', schema,
    Column('id', Integer, primary_key=True),
    Column('username', String(40), nullable=False, unique=True),
    Column('password_hash', String(200), nullable=False),
    Column('role', String(20), nullable=False))
cases = Table('business_cases', schema,
    Column('id', Integer, primary_key=True),
    Column('slug', String(64), nullable=False, unique=True),
    Column('title', String(120), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('author', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
runs = Table('business_runs', schema,
    Column('id', Integer, primary_key=True),
    Column('case_id', ForeignKey('business_cases.id'), nullable=False, index=True),
    Column('status', String(20), nullable=False, index=True),
    Column('owner', String(40), nullable=False),
    Column('attempt_token', String(36)),
    Column('started_at', String(32)),
    Column('finished_at', String(32)),
    Column('created_at', String(32), nullable=False),
    # -1 = 期初未落账；0..N = 已落账的修订条数（断点）。
    Column('processed', Integer, nullable=False, server_default='-1'),
    Column('error', Text))
evidence = Table('business_evidence', schema,
    Column('id', Integer, primary_key=True),
    Column('run_id', ForeignKey('business_runs.id'), nullable=False, unique=True),
    Column('report', JSON, nullable=False),
    Column('digest', String(64), nullable=False),
    Column('reviewer', String(40)),
    Column('review_note', String(500)),
    Column('reviewed_at', String(32)))
audit = Table('audit_events', schema,
    Column('id', Integer, primary_key=True),
    Column('actor', String(40), nullable=False),
    Column('action', String(32), nullable=False),
    Column('reference', String(50), nullable=False),
    Column('created_at', String(32), nullable=False))
# Durable per-run checkpoints: let an interrupted run resume from the last
# posted revision instead of restarting, and are removed when the run finishes.
run_ledger = Table('gb5311_run_ledger', schema,
    Column('run_id', ForeignKey('business_runs.id'), nullable=False),
    Column('batch_id', String(64), nullable=False),
    Column('revision', Integer, nullable=False),
    Column('action', String(8), nullable=False),
    Column('grade', String(64), nullable=False),
    Column('weight_g', Integer, nullable=False),
    Column('pieces', Integer, nullable=False),
    PrimaryKeyConstraint('run_id', 'batch_id', 'revision'))
run_stock = Table('gb5311_run_stock', schema,
    Column('run_id', ForeignKey('business_runs.id'), nullable=False),
    Column('grade', String(64), nullable=False),
    Column('weight_g', BigInteger, nullable=False),
    Column('pieces', BigInteger, nullable=False),
    PrimaryKeyConstraint('run_id', 'grade'))
run_trail = Table('gb5311_run_trail', schema,
    Column('run_id', ForeignKey('business_runs.id'), nullable=False),
    Column('seq', Integer, nullable=False),
    Column('detail', JSON, nullable=False),
    PrimaryKeyConstraint('run_id', 'seq'))


def migrate(engine):
    """Idempotent upgrades for deployments created before a column existed."""
    with engine.begin() as conn:
        columns = {row[0] for row in conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'business_runs'"))}
        if 'processed' not in columns:
            conn.execute(text('ALTER TABLE business_runs ADD COLUMN processed INTEGER NOT NULL DEFAULT -1'))
