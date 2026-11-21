"""Durable per-run checkpoints: resume an interrupted run without double counting.

Mirrors src.services.cloth_stock on run-scoped durable tables. Every state
change commits in one transaction together with a fencing update of
business_runs.processed, so a crashed attempt can be re-claimed and resumed
from the last posted revision, while a superseded attempt (stale token) can
no longer write stock or finish the run.
"""

from sqlalchemy import select, text

from src.constants.workflow import Status
from src.models.tables import run_ledger, run_stock, run_trail, runs


class Fenced(Exception):
    """This attempt no longer owns the run; a newer attempt has taken over."""


REGISTER = text("""
    INSERT INTO gb5311_run_ledger (run_id, batch_id, revision, action, grade, weight_g, pieces)
    VALUES (:run_id, :batch_id, :revision, :action, :grade, :weight_g, :pieces)
    ON CONFLICT (run_id, batch_id, revision) DO NOTHING
    RETURNING revision
""")
SAME_REVISION = text("""
    SELECT batch_id, revision, action, grade, weight_g, pieces
    FROM gb5311_run_ledger
    WHERE run_id = :run_id AND batch_id = :batch_id AND revision = :revision
""")
PREVIOUS_LATEST = text("""
    SELECT revision, action, grade, weight_g, pieces
    FROM gb5311_run_ledger
    WHERE run_id = :run_id AND batch_id = :batch_id AND revision <> :revision
    ORDER BY revision DESC LIMIT 1
""")
RETRACT = text("""
    UPDATE gb5311_run_stock
    SET weight_g = weight_g - :weight_g, pieces = pieces - :pieces
    WHERE run_id = :run_id AND grade = :grade
    RETURNING weight_g
""")
REMOVE_EMPTY_GRADE = text("""
    DELETE FROM gb5311_run_stock WHERE run_id = :run_id AND grade = :grade AND weight_g = 0 AND pieces = 0
""")
ADD = text("""
    INSERT INTO gb5311_run_stock AS stock (run_id, grade, weight_g, pieces)
    VALUES (:run_id, :grade, :weight_g, :pieces)
    ON CONFLICT (run_id, grade) DO UPDATE
    SET weight_g = stock.weight_g + EXCLUDED.weight_g, pieces = stock.pieces + EXCLUDED.pieces
""")

# Same reconciliation as cloth_stock, scoped to one run. The incremental side
# is filtered in a subquery so the FULL JOIN keeps grades missing on either side.
LATEST_FROM_HISTORY = """
    WITH latest AS (
        SELECT DISTINCT ON (batch_id) batch_id, revision, action, grade, weight_g, pieces
        FROM gb5311_run_ledger
        WHERE run_id = :run_id
        ORDER BY batch_id, revision DESC
    )
"""
RECONCILE = text(LATEST_FROM_HISTORY + """
    , full_stock AS (
        SELECT grade, SUM(weight_g) AS weight_g, SUM(pieces) AS pieces
        FROM latest WHERE action = '记账' GROUP BY grade
    )
    SELECT COALESCE(i.grade, f.grade) AS grade,
           i.weight_g AS incremental_weight, i.pieces AS incremental_pieces,
           f.weight_g AS full_weight, f.pieces AS full_pieces,
           (i.weight_g IS DISTINCT FROM f.weight_g OR i.pieces IS DISTINCT FROM f.pieces) AS differs
    FROM (SELECT grade, weight_g, pieces FROM gb5311_run_stock WHERE run_id = :run_id) AS i
    FULL JOIN full_stock AS f USING (grade)
    ORDER BY grade COLLATE "C"
""")
ACTIVE_BATCHES = text(LATEST_FROM_HISTORY + """
    SELECT batch_id, revision, grade, weight_g, pieces FROM latest WHERE action = '记账'
    ORDER BY weight_g DESC, pieces DESC, batch_id COLLATE "C"
""")


def audit_row(index, event, status, retracted, added):
    row = {'流水行': index, '批次': event['batch_id'], '修订号': event['revision'],
           '动作': event['action'], '判定': status}
    for label, contribution in [('撤回', retracted), ('累加', added)]:
        row[f'{label}等级'] = contribution['grade'] if contribution else ''
        row[f'{label}重量（克）'] = contribution['weight_g'] if contribution else 0
        row[f'{label}件数'] = contribution['pieces'] if contribution else 0
    return row


def cleanup(conn, run_id):
    """Drop all checkpoint state of a finished run; safe before any checkpoint existed."""
    for table in (run_trail, run_stock, run_ledger):
        conn.execute(table.delete().where(table.c.run_id == run_id))


class Store:
    """Checkpointed stock for one run attempt, fenced by the attempt token."""

    def __init__(self, engine, run_id, token):
        self.engine = engine
        self.run_id = run_id
        self.token = token

    def _state(self, conn):
        return conn.execute(select(runs.c.processed, runs.c.attempt_token, runs.c.status)
                            .where(runs.c.id == self.run_id)).one()

    def _check_owner(self, state):
        if state.attempt_token != self.token or state.status != Status.RUNNING:
            raise Fenced(self.run_id)

    def _advance(self, conn, target, expected):
        """Fencing gate: commit progress only while this attempt still owns the run."""
        count = conn.execute(runs.update().where(
            runs.c.id == self.run_id, runs.c.attempt_token == self.token,
            runs.c.status == Status.RUNNING, runs.c.processed == expected
        ).values(processed=target)).rowcount
        if count != 1:
            raise Fenced(self.run_id)

    def initialize(self, opening_batches):
        """Post the opening stock once; return how many revisions are already posted."""
        with self.engine.begin() as conn:
            state = self._state(conn)
            self._check_owner(state)
            if state.processed >= 0:
                return state.processed
            for batch in opening_batches:
                event = {**batch, 'run_id': self.run_id, 'action': '记账'}
                conn.execute(REGISTER, event)
                conn.execute(ADD, event)
            self._advance(conn, 0, expected=-1)
            return 0

    def apply(self, index, event):
        """Post one revision entry; return its outcome, or None if already posted."""
        with self.engine.begin() as conn:
            state = self._state(conn)
            self._check_owner(state)
            if state.processed >= index:
                return None
            params = {**event, 'run_id': self.run_id}
            if conn.execute(REGISTER, params).scalar_one_or_none() is None:
                stored = conn.execute(SAME_REVISION, params).mappings().one()
                if dict(stored) != event:
                    # Not posted: the conflict leaves the checkpoint untouched,
                    # so a resumed attempt reports the same first conflict.
                    return {'status': '版本冲突', 'stored': dict(stored)}
                status, retracted, added = '重复', None, None
            else:
                previous = conn.execute(PREVIOUS_LATEST, params).mappings().one_or_none()
                if previous is not None and previous['revision'] > event['revision']:
                    status, retracted, added = '过期', None, None
                else:
                    retracted = previous if previous is not None and previous['action'] == '记账' else None
                    added = event if event['action'] == '记账' else None
                    if retracted is not None:
                        remaining = conn.execute(RETRACT, {**dict(retracted), 'run_id': self.run_id}).scalar_one_or_none()
                        if remaining is None:
                            raise RuntimeError(f"增量库存缺少应撤回的等级 {retracted['grade']}")
                        if remaining == 0:
                            conn.execute(REMOVE_EMPTY_GRADE, {**dict(retracted), 'run_id': self.run_id})
                    if added is not None:
                        conn.execute(ADD, params)
                    status = '生效'
            conn.execute(run_trail.insert().values(
                run_id=self.run_id, seq=index, detail=audit_row(index, event, status, retracted, added)))
            self._advance(conn, index, expected=index - 1)
            return {'status': status}

    def trail(self, conn=None):
        """Posted audit rows in entry order."""
        if conn is None:
            with self.engine.connect() as owned:
                return self.trail(owned)
        rows = conn.execute(select(run_trail.c.detail).where(run_trail.c.run_id == self.run_id)
                            .order_by(run_trail.c.seq)).scalars()
        return [dict(row) for row in rows]
