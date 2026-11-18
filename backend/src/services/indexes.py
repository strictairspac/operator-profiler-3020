"""索引管理服务：页面建/删/重试，Redis 建设队列，崩溃恢复。"""
import logging
import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.services.indexing import core, store
from src.services.indexing.orm_repo import SessionRepo

INDEX_QUEUE = 'operator-index-builds'
queue = redis.Redis.from_url(REDIS_URL)
repo = SessionRepo()


def list_items(dataset_id=None):
    with Session() as s:
        return repo.list_meta(s, dataset_id=dataset_id)


def create_index(data, user):
    fields = core.normalize_fields(data['fields'])
    with Session() as s:
        dataset = s.get(Dataset, data['dataset_id'])
        if not dataset:
            raise HTTPException(404, '未找到数据集')
        existing = repo.find_meta(s, dataset.id, core.fields_key(fields))
        if existing:
            if existing['status'] == 'building':
                raise HTTPException(409, '该字段索引正在建设中，请勿重复提交')
            # 已存在 available/failed：重新建设复用同一行，重试不重复建索引。
            index_id = existing['id']
            repo.update_meta(s, index_id, status='building', version=None,
                             entry_count=0, checksum=None, error=None)
            repo.delete_entries(s, index_id)
        else:
            meta = repo.create_meta(s, dataset.id, fields)
            index_id = meta['id']
        s.commit()
    _enqueue(index_id)
    return _get(index_id)


def retry_index(index_id):
    with Session() as s:
        meta = repo.get_meta(s, index_id)
        if not meta:
            raise HTTPException(404, '未找到索引')
        if meta['status'] == 'building':
            raise HTTPException(409, '索引正在建设中，无需重试')
        repo.update_meta(s, index_id, status='building', version=None,
                         entry_count=0, checksum=None, error=None)
        repo.delete_entries(s, index_id)
        s.commit()
    _enqueue(index_id)
    return _get(index_id)


def delete_index(index_id):
    with Session() as s:
        meta = repo.get_meta(s, index_id)
        if not meta:
            raise HTTPException(404, '未找到索引')
        repo.delete_meta(s, index_id)
        s.commit()
    return {'deleted': index_id}


def build_index_job(index_id):
    """worker 调用：含崩溃恢复语义的建设任务入口。"""
    with Session() as s:
        ok, meta = store.build_and_publish(repo, s, index_id)
    if not ok:
        logging.warning('index build failed: %s', index_id)
    return ok, meta


def recover_stuck_builds():
    """worker 启动时调用：崩溃遗留的 building 索引重新入队，避免重复建设。

    恢复是权威动作：先清除可能残留的任务去重锁，再强制入队，
    否则崩溃前未释放的锁会让恢复消息无法投递。
    """
    with Session() as s:
        stuck = repo.reset_stuck_building(s)
        ids = [m['id'] for m in stuck]
    for index_id in ids:
        _enqueue(index_id, force=True)
    return ids


def open_catalog(snapshot, on_corrupt=None):
    """执行期按受理快照装载可用且完整的索引目录。

    损坏索引在独立事务里登记为 failed（不影响当前执行的读事务），随后跳过，
    执行自动回退全扫。
    """
    def handle(meta_id, message):
        with Session() as s:
            store.mark_failed(repo, s, meta_id, '索引损坏：' + message)
        if on_corrupt:
            on_corrupt(meta_id, message)

    with Session() as s:
        payload = dict(snapshot)
        payload['_on_corrupt'] = handle
        return store.load_catalog(repo, s, payload)


def _get(index_id):
    with Session() as s:
        return repo.get_meta(s, index_id)


def _enqueue(index_id, force=False):
    # SET NX 去重键防止重试/恢复期间同一索引重复入队；消费后释放。
    # force 用于崩溃恢复：接管残留锁后强制投递。
    lock_key = 'index-job-lock:%s' % index_id
    try:
        if force:
            queue.set(lock_key, '1', ex=3600)
        elif not queue.set(lock_key, '1', nx=True, ex=3600):
            return
        queue.rpush(INDEX_QUEUE, index_id)
    except redis.RedisError:
        with Session() as s:
            repo.update_meta(s, index_id, status='failed', error='建设队列不可用，请稍后重试')
            s.commit()
        raise HTTPException(503, '建设队列不可用，请稍后重试')


def release_job_lock(index_id):
    try:
        queue.delete('index-job-lock:%s' % index_id)
    except redis.RedisError:
        logging.warning('release index job lock failed: %s', index_id)
