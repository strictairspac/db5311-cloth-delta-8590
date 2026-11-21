import logging
from pathlib import Path
from threading import Event, Thread
from time import sleep

from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import ARTIFACTS, cache, engine
from src.services.domain import solve
from src.services.run_stock import Fenced
from src.services.workflow import claim, finish, requeue

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def work():
    HEARTBEAT.unlink(missing_ok=True)
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            thread = Thread(target=heartbeat, args=(stop,), daemon=True)
            thread.start()
            try:
                result = solve(payload, engine, run_id=run_id, token=token)
                finish(run_id, token, result=result)
            except Fenced:
                # A newer attempt owns the run now; this attempt must not write.
                logging.info('Run %s taken over by a newer attempt', run_id)
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except SQLAlchemyError:
                # Interrupted mid-run: keep posted checkpoints and resume the same run later.
                logging.exception('Run %s interrupted, requeueing for resume', run_id)
                requeue(run_id, token)
                sleep(2)
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
            finally:
                stop.set()
                thread.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
