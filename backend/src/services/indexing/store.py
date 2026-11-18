"""索引持久化仓储与可恢复构建状态机。

仓储抽象不绑定具体驱动：生产环境用 SQLAlchemy/PostgreSQL 适配器（SessionRepo），
验证环境用标准库 sqlite3 适配器（SqliteRepo），二者实现同一组方法，
保证建设、发布、恢复、损坏回退的行为在两种驱动下一致。
"""
from src.services.indexing import core


# --------------------------------------------------------------------------- #
# 仓储协议：元数据行均以 dict 返回
# --------------------------------------------------------------------------- #
class BaseRepo:
    def get_dataset(self, session, dataset_id):
        raise NotImplementedError

    def lock_dataset(self, session, dataset_id):
        """发布前锁住数据集行，与并发保存串行化；不支持行锁的驱动退化为普通读取。"""
        return self.get_dataset(session, dataset_id)

    def get_meta(self, session, index_id):
        raise NotImplementedError

    def list_meta(self, session, dataset_id=None, status=None):
        raise NotImplementedError

    def find_meta(self, session, dataset_id, fields_key):
        raise NotImplementedError

    def create_meta(self, session, dataset_id, fields):
        raise NotImplementedError

    def update_meta(self, session, index_id, **changes):
        raise NotImplementedError

    def lock_meta(self, session, index_id):
        """发布/重试前锁住元数据行；不支持行锁的驱动退化为普通读取。"""
        return self.get_meta(session, index_id)

    def count_entries(self, session, index_id, phase):
        raise NotImplementedError

    def insert_entries(self, session, index_id, entries):
        raise NotImplementedError

    def promote_entries(self, session, index_id):
        """staged -> active，与元数据发布在同一事务内提交。"""
        raise NotImplementedError

    def delete_entries(self, session, index_id):
        raise NotImplementedError

    def delete_meta(self, session, index_id):
        raise NotImplementedError

    def reset_stuck_building(self, session):
        """进程崩溃恢复：把无人持锁的 building 索引交还给可重试队列。"""
        raise NotImplementedError

    def entries_for(self, session, index_id):
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# 可恢复构建与发布
# --------------------------------------------------------------------------- #
def build_and_publish(repo, session, index_id, max_attempts=3):
    """建设并发布一个索引；发布时覆盖建设期间所有已保存的数据修改。

    - 数据在发布事务内重新读取并加锁，版本不符则在同一任务内重试，不新增索引。
    - 条目先以 staged 落库（建设中断后可清理重建，不重复堆积），
      发布事务里整体 promote 为 active，半成品永不参与执行。
    - 成功返回 (True, meta)；不可恢复失败返回 (False, meta)。
    """
    for attempt in range(1, max_attempts + 1):
        meta = repo.lock_meta(session, index_id)
        if not meta:
            return False, None
        if meta['status'] == 'available':
            return True, meta  # 幂等：已发布（例如重复投递的任务）
        if meta['status'] not in ('building', 'failed'):
            return False, meta
        repo.update_meta(session, index_id, status='building', error=None)
        dataset = repo.get_dataset(session, meta['dataset_id'])
        if dataset is None:
            repo.update_meta(session, index_id, status='failed', error='数据集已删除，无法建设索引')
            return False, repo.get_meta(session, index_id)
        fields = core.normalize_fields(meta['fields'])
        # 上次中断遗留的半成品先清理，再全量重建，保证重试不重复建条目。
        repo.delete_entries(session, index_id)
        entries = core.build_entries(dataset, fields)
        repo.insert_entries(session, index_id, entries)
        version = core.dataset_checksum(dataset)
        checksum = core.entries_checksum(entries)
        # 发布前锁住数据集行并复核版本：并发保存要么在锁前完成（被本次读取覆盖），
        # 要么在锁后等待（其修改属于发布之后的新版本，执行时按版本不符回退全扫）。
        current = repo.lock_dataset(session, meta['dataset_id'])
        if core.dataset_checksum(current) != version:
            # 建设期间数据集被保存：本轮不发布，下一轮先 DELETE 回收 staged 半成品，
            # 再按最新数据全量重建；整个任务只在成功时提交一次，半成品永不 active。
            continue
        repo.promote_entries(session, index_id)
        repo.update_meta(
            session, index_id, status='available', version=version,
            entry_count=len(entries), checksum=checksum, error=None)
        session.commit()
        return True, repo.get_meta(session, index_id)
    meta = repo.get_meta(session, index_id)
    repo.update_meta(session, index_id, status='failed',
                     error='建设期间数据集持续被编辑，超过重试上限，请稍后重试')
    session.commit()
    return False, repo.get_meta(session, index_id)


def mark_failed(repo, session, index_id, message):
    """执行期发现损坏时登记失败状态（不删除旧 active 条目之外的信息）。"""
    meta = repo.get_meta(session, index_id)
    if meta and meta['status'] != 'failed':
        repo.update_meta(session, index_id, status='failed', error=message[:1000])
        session.commit()


# --------------------------------------------------------------------------- #
# 执行期：按受理快照选索引
# --------------------------------------------------------------------------- #
def load_catalog(repo, session, dataset_snapshot):
    """返回与受理快照版本一致、状态 available 的索引，并完成完整性校验。

    版本不符直接跳过（旧快照/编辑后新索引均不入选）；
    校验抛错的索引通过 on_corrupt 回调登记为 failed 并跳过。
    """
    snapshot_version = core.dataset_checksum(dataset_snapshot)
    catalog = []
    for meta in repo.list_meta(session, status='available'):
        if meta['dataset_id'] != dataset_snapshot.get('id') or meta['version'] != snapshot_version:
            continue
        try:
            entries = [tuple(e) for e in repo.entries_for(session, meta['id'])]
            index = {
                'index_id': meta['id'], 'fields': meta['fields'], 'entries': entries,
                'entry_count': meta['entry_count'], 'checksum': meta['checksum'],
            }
            core.verify_index(index, dataset_snapshot)
        except core.IndexCorrupt as exc:
            on_corrupt = dataset_snapshot.get('_on_corrupt')
            if on_corrupt:
                on_corrupt(meta['id'], str(exc))
            continue
        catalog.append(index)
    return catalog
