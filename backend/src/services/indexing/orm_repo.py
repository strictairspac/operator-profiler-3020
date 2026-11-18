"""SQLAlchemy / PostgreSQL 仓储适配器。"""
import json
from sqlalchemy import text
from src.models.index import IndexMeta, IndexEntry
from src.services.indexing.store import BaseRepo


def _meta_dict(row):
    return {c.name: getattr(row, c.name) for c in IndexMeta.__table__.columns}


class SessionRepo(BaseRepo):
    def get_dataset(self, s, dataset_id):
        from src.models.dataset import Dataset
        row = s.get(Dataset, dataset_id)
        if not row:
            return None
        return {'id': row.id, 'name': row.name, 'description': row.description,
                'sensors': row.sensors, 'readings': row.readings}

    def lock_dataset(self, s, dataset_id):
        from src.models.dataset import Dataset
        row = s.query(Dataset).filter(Dataset.id == dataset_id).with_for_update().first()
        if not row:
            return None
        return {'id': row.id, 'name': row.name, 'description': row.description,
                'sensors': row.sensors, 'readings': row.readings}

    def get_meta(self, s, index_id):
        row = s.get(IndexMeta, index_id)
        return _meta_dict(row) if row else None

    def list_meta(self, s, dataset_id=None, status=None):
        q = s.query(IndexMeta)
        if dataset_id is not None:
            q = q.filter(IndexMeta.dataset_id == dataset_id)
        if status is not None:
            q = q.filter(IndexMeta.status == status)
        return [_meta_dict(r) for r in r_all(q)]

    def find_meta(self, s, dataset_id, fields_key):
        row = s.query(IndexMeta).filter_by(dataset_id=dataset_id, fields_key=fields_key).first()
        return _meta_dict(row) if row else None

    def create_meta(self, s, dataset_id, fields):
        row = IndexMeta(dataset_id=dataset_id, fields=list(fields),
                        fields_key=','.join(fields), status='building')
        s.add(row)
        s.flush()
        return _meta_dict(row)

    def update_meta(self, s, index_id, **changes):
        row = s.get(IndexMeta, index_id)
        for k, v in changes.items():
            setattr(row, k, v)
        s.flush()
        return _meta_dict(row)

    def lock_meta(self, s, index_id):
        row = s.query(IndexMeta).filter(IndexMeta.id == index_id).with_for_update().first()
        return _meta_dict(row) if row else None

    def count_entries(self, s, index_id, phase):
        return s.query(IndexEntry).filter_by(index_id=index_id, phase=phase).count()

    def insert_entries(self, s, index_id, entries):
        s.bulk_insert_mappings(IndexEntry, [
            {'index_id': index_id, 'phase': 'staged', 'pos': e[-1],
             'key_json': json.dumps(list(e[:-1]), separators=(',', ':'))}
            for e in entries])

    def promote_entries(self, s, index_id):
        s.query(IndexEntry).filter_by(index_id=index_id).update(
            {'phase': 'active'}, synchronize_session=False)

    def delete_entries(self, s, index_id):
        s.query(IndexEntry).filter_by(index_id=index_id).delete(synchronize_session=False)

    def delete_meta(self, s, index_id):
        self.delete_entries(s, index_id)
        row = s.get(IndexMeta, index_id)
        if row:
            s.delete(row)
        s.flush()

    def reset_stuck_building(self, s):
        # 崩溃恢复只负责把 building 元数据暴露为可重试；队列投递由服务层负责。
        rows = s.query(IndexMeta).filter(IndexMeta.status == 'building').all()
        return [_meta_dict(r) for r in rows]

    def entries_for(self, s, index_id):
        rows = s.query(IndexEntry).filter_by(index_id=index_id, phase='active').order_by(IndexEntry.id)
        out = []
        for r in rows:
            out.append(tuple(json.loads(r.key_json)) + (r.pos,))
        return out


def r_all(query):
    return query.order_by(IndexMeta.id.asc()).all()
