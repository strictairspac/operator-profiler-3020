import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services.operators import execute
from src.services import indexes
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
QUEUES={'operator-traces':'trace','operator-index-builds':'index'}

def recover():
 # 进程崩溃后 running 的 trace 按原约定保留人工复查；building 的索引重新入队。
 try:return indexes.recover_stuck_builds()
 except Exception:logging.exception('index recovery failed');return []

def run_trace(run_id):
 with Session() as s:
  row=s.get(Trace,run_id)
  if not row or row.status!='queued':return
  row.status='running';snapshot=row.snapshot;s.commit()
 try:
  catalog=indexes.open_catalog(snapshot['dataset'])
  result=execute(snapshot['dataset'],snapshot['plan']['steps'],index_catalog=catalog)
  with Session() as s:r=s.get(Trace,run_id);r.result=result;r.status='completed';s.commit()
  log('worker','trace.completed',str(run_id))
 except Exception as exc:
  logging.exception('trace failed')
  with Session() as s:r=s.get(Trace,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()

def run_index_job(index_id):
 try:indexes.build_index_job(index_id)
 except Exception:logging.exception('index build job failed')
 finally:indexes.release_job_lock(index_id)

recover()
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop(list(QUEUES),timeout=2)
  if not item:continue
  kind=QUEUES[item[0].decode() if isinstance(item[0],bytes) else item[0]]
  payload=int(item[1])
  if kind=='trace':run_trace(payload)
  else:run_index_job(payload)
 except Exception:logging.exception('worker loop');time.sleep(2)
