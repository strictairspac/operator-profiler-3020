"""索引子系统的标准库端到端验证（无需 PostgreSQL/Redis/第三方包）。

运行：cd backend && python3 -I tests/test_indexing.py
SqliteRepo 与生产 SessionRepo 实现同一仓储协议，验证建设状态机、发布覆盖、
崩溃恢复、版本选择、损坏回退与执行结果等价性在真实持久化（sqlite 文件）下成立。
"""
import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.indexing import core, store
from src.services import operators


# --------------------------------------------------------------------------- #
# sqlite 仓储：列与 src/models/index.py、dataset.py 对齐
# --------------------------------------------------------------------------- #
class SqliteSession:
    """显式事务会话：BEGIN IMMEDIATE 后写入不自动落库，commit/rollback 真实生效，
    与 PostgreSQL 适配器的事务语义对齐（发布整体可见、版本不符可整轮回滚）。"""
    def __init__(self, conn):
        self.conn = conn
        self.conn.isolation_level = None  # 关闭模块级自动事务，手动控制
        self.conn.execute('BEGIN IMMEDIATE')

    def commit(self):
        self.conn.commit()
        self.conn.execute('BEGIN IMMEDIATE')

    def rollback(self):
        self.conn.rollback()
        self.conn.execute('BEGIN IMMEDIATE')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False


class SqliteRepo(store.BaseRepo):
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.isolation_level = None  # 关闭模块级隐式事务，全程手动 BEGIN/COMMIT
        self.conn.execute('PRAGMA foreign_keys=ON')
        self.conn.execute('BEGIN')
        for ddl in (
            "CREATE TABLE IF NOT EXISTS datasets("
            " id INTEGER PRIMARY KEY, name TEXT UNIQUE, description TEXT DEFAULT '',"
            " sensors TEXT NOT NULL, readings TEXT NOT NULL)",
            'CREATE TABLE IF NOT EXISTS index_meta('
            ' id INTEGER PRIMARY KEY, dataset_id INTEGER NOT NULL,'
            ' fields TEXT NOT NULL, fields_key TEXT NOT NULL, status TEXT NOT NULL,'
            ' version TEXT, entry_count INTEGER NOT NULL DEFAULT 0,'
            ' checksum TEXT, error TEXT,'
            ' UNIQUE(dataset_id, fields_key))',
            'CREATE TABLE IF NOT EXISTS index_entries('
            ' id INTEGER PRIMARY KEY, index_id INTEGER NOT NULL,'
            " phase TEXT NOT NULL, pos INTEGER NOT NULL, key_json TEXT NOT NULL)"):
            self.conn.execute(ddl)
        self.conn.commit()
        self.conn.execute('BEGIN IMMEDIATE')

    def session(self):
        return SqliteSession(self.conn)

    # -- datasets ----------------------------------------------------------- #
    def get_dataset(self, s, dataset_id):
        row = self.conn.execute('SELECT * FROM datasets WHERE id=?', (dataset_id,)).fetchone()
        if not row:
            return None
        return {'id': row['id'], 'name': row['name'], 'description': row['description'],
                'sensors': json.loads(row['sensors']), 'readings': json.loads(row['readings'])}

    def save_dataset(self, dataset):
        with self.session() as tx:
            tx.conn.execute(
                'INSERT INTO datasets(name,description,sensors,readings) VALUES(?,?,?,?)',
                (dataset['name'], dataset.get('description', ''),
                 json.dumps(dataset['sensors'], ensure_ascii=False),
                 json.dumps(dataset['readings'], ensure_ascii=False)))
        return self.conn.execute('SELECT id FROM datasets WHERE name=?', (dataset['name'],)).fetchone()['id']

    def update_readings(self, dataset_id, readings):
        with self.session() as tx:
            tx.conn.execute('UPDATE datasets SET readings=? WHERE id=?',
                            (json.dumps(readings, ensure_ascii=False), dataset_id))

    def tamper_entry(self, index_id):
        with self.session() as tx:
            tx.conn.execute(
                'UPDATE index_entries SET key_json=? WHERE id=(SELECT min(id) FROM index_entries WHERE index_id=?)',
                ('[999999]', index_id))

    # -- index meta --------------------------------------------------------- #
    def _meta(self, row):
        if not row:
            return None
        return {'id': row['id'], 'dataset_id': row['dataset_id'],
                'fields': json.loads(row['fields']), 'fields_key': row['fields_key'],
                'status': row['status'], 'version': row['version'],
                'entry_count': row['entry_count'], 'checksum': row['checksum'],
                'error': row['error']}

    def get_meta(self, s, index_id):
        return self._meta(self.conn.execute('SELECT * FROM index_meta WHERE id=?', (index_id,)).fetchone())

    def list_meta(self, s, dataset_id=None, status=None):
        sql, args = 'SELECT * FROM index_meta WHERE 1=1', []
        if dataset_id is not None:
            sql += ' AND dataset_id=?'; args.append(dataset_id)
        if status is not None:
            sql += ' AND status=?'; args.append(status)
        sql += ' ORDER BY id'
        return [self._meta(r) for r in self.conn.execute(sql, args).fetchall()]

    def find_meta(self, s, dataset_id, fields_key):
        return self._meta(self.conn.execute(
            'SELECT * FROM index_meta WHERE dataset_id=? AND fields_key=?',
            (dataset_id, fields_key)).fetchone())

    def create_meta(self, s, dataset_id, fields):
        fields = tuple(fields)
        self.conn.execute(
            'INSERT INTO index_meta(dataset_id,fields,fields_key,status) VALUES(?,?,?,?)',
            (dataset_id, json.dumps(list(fields)), ','.join(fields), 'building'))
        return self.get_meta(s, self.conn.execute('SELECT last_insert_rowid() id').fetchone()['id'])

    def update_meta(self, s, index_id, **changes):
        if not changes:
            return self.get_meta(s, index_id)
        sets, args = [], []
        for k, v in changes.items():
            sets.append('%s=?' % k)
            args.append(json.dumps(v, ensure_ascii=False) if k == 'fields' else v)
        args.append(index_id)
        self.conn.execute('UPDATE index_meta SET %s WHERE id=?' % ','.join(sets), args)
        return self.get_meta(s, index_id)

    def lock_meta(self, s, index_id):
        return self.get_meta(s, index_id)

    # -- entries ------------------------------------------------------------ #
    def count_entries(self, s, index_id, phase):
        return self.conn.execute(
            'SELECT COUNT(*) c FROM index_entries WHERE index_id=? AND phase=?',
            (index_id, phase)).fetchone()['c']

    def insert_entries(self, s, index_id, entries):
        self.conn.executemany(
            'INSERT INTO index_entries(index_id,phase,pos,key_json) VALUES(?,?,?,?)',
            [(index_id, 'staged', e[-1], json.dumps(list(e[:-1]), separators=(',', ':'))) for e in entries])

    def promote_entries(self, s, index_id):
        self.conn.execute("UPDATE index_entries SET phase='active' WHERE index_id=?", (index_id,))

    def delete_entries(self, s, index_id):
        self.conn.execute('DELETE FROM index_entries WHERE index_id=?', (index_id,))

    def delete_meta(self, s, index_id):
        self.delete_entries(s, index_id)
        self.conn.execute('DELETE FROM index_meta WHERE id=?', (index_id,))

    def reset_stuck_building(self, s):
        return self.list_meta(s, status='building')

    def entries_for(self, s, index_id):
        rows = self.conn.execute(
            "SELECT key_json,pos FROM index_entries WHERE index_id=? AND phase='active' ORDER BY id",
            (index_id,)).fetchall()
        return [tuple(json.loads(r['key_json'])) + (r['pos'],) for r in rows]

    def tamper_entry(self, index_id):
        with self.session() as tx:
            tx.conn.execute(
                'UPDATE index_entries SET key_json=? WHERE id=(SELECT min(id) FROM index_entries WHERE index_id=?)',
                ('[999999]', index_id))


class FlipRepo(SqliteRepo):
    """前 swap_after_reads 次读取返回旧数据集，之后返回新数据集，模拟建设期间并发保存。

    仅覆写 get_dataset，其余仓储行为（含独立 get_meta 读取）与生产适配器一致。
    """
    @classmethod
    def wrap(cls, inner, old, new, swap_after_reads):
        obj = cls.__new__(cls)
        obj.__dict__ = inner.__dict__.copy()
        obj._flip_state = {'calls': 0}
        obj.old, obj.new, obj.swap_after_reads = old, new, swap_after_reads
        return obj

    def get_dataset(self, s, dataset_id):
        # 仅统计数据读取：第 1 次（attempt1 构建）返回旧数据，
        # 之后（attempt1 复核及 attempt2）返回建设期间并发保存的新数据。
        self._flip_state['calls'] += 1
        return self.old if self._flip_state['calls'] <= 1 else self.new


PASS = 0


def check(name, cond, extra=''):
    global PASS
    if not cond:
        raise AssertionError('FAIL %s %s' % (name, extra))
    PASS += 1
    print('  ok -', name)


def section(title):
    print('\n== %s' % title)


# --------------------------------------------------------------------------- #
# 构造数据
# --------------------------------------------------------------------------- #
def make_dataset(n=120, duplicate=True):
    sensors = [{'sensor_id': i, 'zone': 'z%d' % (i % 3), 'device_type': 'pump'} for i in range(1, 13)]
    readings = []
    for i in range(n):
        readings.append({'sensor_id': i % 12 + 1,
                         'value': round(20 + (i * 37 % 800) / 10 + 0.1234567, 7),
                         'recorded_at': '2026-11-17T%02d:%02d:00' % (8 + i // 60, i % 60)})
    if duplicate:
        readings.append(dict(readings[0]))  # 完全重复记录
        readings.append(dict(readings[0]))
    return {'name': 'ds-%d' % n, 'sensors': sensors, 'readings': readings}


STEPS_EQ = [{'op': 'scan', 'source': 'readings'},
            {'op': 'filter', 'field': 'sensor_id', 'compare': 'eq', 'value': 7}]
STEPS_RANGE = [{'op': 'scan', 'source': 'readings'},
               {'op': 'filter', 'field': 'value', 'compare': 'gte', 'value': 70},
               {'op': 'filter', 'field': 'value', 'compare': 'lte', 'value': 85}]
STEPS_COMPOSITE = [{'op': 'scan', 'source': 'readings'},
                   {'op': 'filter', 'field': 'sensor_id', 'compare': 'eq', 'value': 3},
                   {'op': 'filter', 'field': 'value', 'compare': 'gte', 'value': 80}]
STEPS_LOW_SELECTIVITY = [{'op': 'scan', 'source': 'readings'},
                         {'op': 'filter', 'field': 'value', 'compare': 'lte', 'value': 1000000}]
STEPS_FULL_PIPELINE = [
    {'op': 'scan', 'source': 'readings'},
    {'op': 'filter', 'field': 'value', 'compare': 'gte', 'value': 80},
    {'op': 'hash_join', 'right': 'sensors', 'left_key': 'sensor_id', 'right_key': 'sensor_id'},
    {'op': 'filter', 'field': 'zone', 'compare': 'eq', 'value': 'z1'},
    {'op': 'group', 'keys': ['zone'], 'measure': 'value', 'aggregate': 'avg'}]


def rebuild(repo, session, index_id, dataset):
    """模拟服务层重建：先置 building 并清条目提交，再走可恢复建设流程。"""
    repo.update_meta(session, index_id, status='building', version=None,
                     entry_count=0, checksum=None, error=None)
    repo.delete_entries(session, index_id)
    session.commit()
    ok, meta = store.build_and_publish(repo, session, index_id)
    assert ok, meta
    return meta


def main():
    tmp = tempfile.mkdtemp(prefix='idx-test-')
    repo = SqliteRepo(os.path.join(tmp, 'index-test.sqlite'))
    s = repo.session()
    dataset = make_dataset()
    ds_id = repo.save_dataset(dataset)
    dataset['id'] = ds_id
    n_rows = len(dataset['readings'])

    # ------------------------------------------------------------------ #
    section('core：构建、重复记录与六位精度')
    entries_sid = core.build_entries(dataset, ['sensor_id'])
    check('每条 reading 一个条目（含重复记录）', len(entries_sid) == n_rows)
    check('条目按 (键, 位置) 有序', entries_sid == sorted(entries_sid, key=lambda e: e[:-1] + (e[-1],)))
    check('回表位置全覆盖且不缺不重', sorted(e[-1] for e in entries_sid) == list(range(n_rows)))
    entries_val = core.build_entries(dataset, ['value'])
    check('value 键沿用六位精度', all(abs(e[0] - round(e[0], 6)) == 0 for e in entries_val))
    entries_combo = core.build_entries(dataset, ['value', 'sensor_id'])
    check('字段顺序规范化为 sensor_id,value',
          core.normalize_fields(['value', 'sensor_id']) == ('sensor_id', 'value') and
          len(entries_combo[0]) == 3)
    try:
        core.normalize_fields(['zone'])
        check('拒绝不支持字段', False)
    except ValueError:
        check('拒绝不支持字段', True)

    section('core：完整性校验捕获各类损坏')
    good = {'fields': ['sensor_id'], 'entries': entries_sid,
            'entry_count': len(entries_sid), 'checksum': core.entries_checksum(entries_sid)}
    core.verify_index(good, dataset)
    check('完好索引通过校验', True)
    mutated_cases = (
        ('计数不符', dict(good, entry_count=good['entry_count'] - 1)),
        ('校验和不符', dict(good, checksum='0' * 64)),
        ('条目无序', dict(good, entries=[good['entries'][1], good['entries'][0]] + good['entries'][2:])),
        ('越界位置', dict(good, entries=[(1, n_rows + 5)])),
        ('键与源数据不符', dict(good,
                                entries=[(999, 0)] + good['entries'][1:],
                                entry_count=len(good['entries']),
                                checksum=core.entries_checksum([(999, 0)] + good['entries'][1:]))),
        ('覆盖不全', dict(good, entries=good['entries'][:-1],
                          entry_count=len(good['entries']) - 1,
                          checksum=core.entries_checksum(good['entries'][:-1]))))
    for name, mutated in mutated_cases:
        try:
            core.verify_index(mutated, dataset)
            check('识别损坏：' + name, False)
        except core.IndexCorrupt:
            check('识别损坏：' + name, True)

    section('core：等值/范围/组合/矛盾/文本检索')
    got, span = core.seek(entries_sid, ['sensor_id'],
                          [{'field': 'sensor_id', 'compare': 'eq', 'value': 7}])
    expect = [i for i, r in enumerate(dataset['readings']) if r['sensor_id'] == 7]
    check('sensor_id 等值检索位置与全扫一致（集合）', sorted(got) == sorted(expect))
    check('sensor_id 等值保持位置升序输出', got == sorted(expect))
    check('高选择性等值跨越条目明显少于全表', span == len(expect) < n_rows)
    got2, _ = core.seek(entries_val, ['value'], [
        {'field': 'value', 'compare': 'gte', 'value': 60},
        {'field': 'value', 'compare': 'lte', 'value': 90}])
    # 索引键为六位精度：seek 的契约按 round(value,6) 判定；原始值边界由引擎回表复核。
    expect2 = [i for i, r in enumerate(dataset['readings'])
               if 60 <= round(r['value'], 6) <= 90]
    # seek 按键序返回位置；与期望集合比较，顺序由引擎回表时恢复为位置升序。
    check('value 闭区间检索与索引键语义一致', sorted(got2) == sorted(expect2),
          '%d vs %d' % (len(got2), len(expect2)))
    got3, _ = core.seek(entries_combo, ['sensor_id', 'value'], [
        {'field': 'sensor_id', 'compare': 'eq', 'value': 3},
        {'field': 'value', 'compare': 'gte', 'value': 50}])
    expect3 = [i for i, r in enumerate(dataset['readings'])
               if r['sensor_id'] == 3 and round(r['value'], 6) >= 50]
    check('组合索引等值+范围与索引键语义一致', sorted(got3) == sorted(expect3))
    # 六位精度只可能多取（超集），不会漏掉原始值合格行，回表复核后结果精确。
    raw_expect2 = [i for i, r in enumerate(dataset['readings']) if 60 <= r['value'] <= 90]
    check('索引候选是原始谓词真匹配的超集（回表复核保证精确）', set(raw_expect2).issubset(set(got2)))
    check('矛盾谓词返回空候选', core.seek(entries_val, ['value'], [
        {'field': 'value', 'compare': 'gte', 'value': 100},
        {'field': 'value', 'compare': 'lte', 'value': 10}]) == ([], 0))
    check('文本等值不被数值索引服务', core.seek(entries_sid, ['sensor_id'], [
        {'field': 'sensor_id', 'compare': 'eq', 'value': 'x'}]) is None)

    section('core：选择性决策（高选用 / 低选不用）')
    catalog = [{'index_id': 1, 'fields': ['sensor_id'], 'entries': entries_sid,
                'entry_count': len(entries_sid), 'checksum': core.entries_checksum(entries_sid)}]
    chosen, candidates, reason = core.plan_index(
        catalog, [{'field': 'sensor_id', 'compare': 'eq', 'value': 7}], n_rows)
    check('高选择性选择索引', chosen is not None and chosen['index_id'] == 1)
    check('候选给出访问量估算', candidates[0]['estimated_visits'] < n_rows)
    chosen2, _, reason2 = core.plan_index(
        [{'index_id': 2, 'fields': ['value'], 'entries': entries_val,
          'entry_count': len(entries_val), 'checksum': core.entries_checksum(entries_val)}],
        [{'field': 'value', 'compare': 'lte', 'value': 1000000}], n_rows)
    check('低选择性不强用索引', chosen2 is None and '全扫' in reason2)
    chosen3, _, _ = core.plan_index(
        catalog, [{'field': 'zone', 'compare': 'eq', 'value': 'z1'}], n_rows)
    check('无前导谓词不选索引', chosen3 is None)

    # ------------------------------------------------------------------ #
    section('持久化：建设发布、半成品隔离、重启可用')
    meta = repo.create_meta(s, ds_id, core.normalize_fields(['sensor_id']))
    idx_id = meta['id']
    s.commit()
    repo.insert_entries(s, idx_id, entries_sid)
    check('建设中只有 staged，执行读不到半成品', repo.entries_for(s, idx_id) == [])
    check('建设中状态不进入可用目录', store.load_catalog(repo, s, dataset) == [])
    ok, meta = store.build_and_publish(repo, s, idx_id)
    check('发布成功', ok and meta['status'] == 'available')
    check('发布后条目整体 active 且数量一致',
          repo.count_entries(s, idx_id, 'active') == n_rows and
          repo.count_entries(s, idx_id, 'staged') == 0)
    check('版本与校验和落库', meta['version'] == core.dataset_checksum(dataset) and meta['checksum'])
    repo.conn.close()
    repo2 = SqliteRepo(repo.path)  # 模拟进程重启
    s2 = repo2.session()
    catalog_loaded = store.load_catalog(repo2, s2, dataset)
    check('重启后索引仍可装载并通过校验',
          len(catalog_loaded) == 1 and catalog_loaded[0]['index_id'] == idx_id)

    section('持久化：建设中断可恢复，重试不重复建索引')
    meta2 = repo2.create_meta(s2, ds_id, core.normalize_fields(['value']))
    repo2.insert_entries(s2, meta2['id'], core.build_entries(dataset, ['value'])[:5])  # 半成品
    s2.commit()
    stuck = repo2.reset_stuck_building(s2)
    check('恢复发现 building 索引', [m['id'] for m in stuck] == [meta2['id']])
    meta_rows_before = len(repo2.list_meta(s2))
    ok2, meta2b = store.build_and_publish(repo2, s2, meta2['id'])
    check('中断后重试建设成功', ok2 and meta2b['status'] == 'available')
    check('重试复用同一索引行，不新增索引', len(repo2.list_meta(s2)) == meta_rows_before)
    check('重试后无 staged 残留且条目不重复',
          repo2.count_entries(s2, meta2['id'], 'staged') == 0 and
          repo2.count_entries(s2, meta2['id'], 'active') == n_rows)
    ok3, meta3 = store.build_and_publish(repo2, s2, meta2['id'])
    check('重复投递幂等返回 available', ok3 and meta3['status'] == 'available' and
          repo2.count_entries(s2, meta2['id'], 'active') == n_rows)

    section('持久化：建设期间并发编辑，发布覆盖期间修改')
    edited = make_dataset(n=60, duplicate=False)
    edited['id'] = ds_id
    meta4 = repo2.create_meta(s2, ds_id, core.normalize_fields(['sensor_id', 'value']))
    s2.commit()
    # 第 1 次数据读取（attempt1 构建）后即发生并发保存，attempt1 复核读到新数据触发重试，
    # attempt2 按最新数据重建并发布（发布覆盖建设期间所有已保存修改）。
    flip = FlipRepo.wrap(repo2, dataset, edited, swap_after_reads=1)
    ok4, meta4b = store.build_and_publish(flip, s2, meta4['id'], max_attempts=3)
    check('并发编辑后任务内重试并发布成功', ok4 and meta4b['status'] == 'available')
    check('发布版本覆盖为最新已保存数据',
          meta4b['version'] == core.dataset_checksum(edited) and
          meta4b['entry_count'] == len(edited['readings']))
    repo2.update_readings(ds_id, edited['readings'])
    combo_loaded = [c for c in store.load_catalog(repo2, s2, edited)
                    if c['fields'] == ['sensor_id', 'value']]
    check('新快照能选到覆盖最新数据的组合索引', len(combo_loaded) == 1)
    # 旧快照：只有版本仍等于旧内容的索引可入选；按新数据发布的组合索引版本不符，不入选。
    old_catalog = store.load_catalog(repo2, s2, dataset)
    check('旧快照选不到新版本组合索引（版本不符）',
          all(c['fields'] != ['sensor_id', 'value'] for c in old_catalog))
    check('旧快照仍可选到版本一致的旧索引（组合索引除外）',
          bool(old_catalog) and
          all(c['fields'] != ['sensor_id', 'value'] for c in old_catalog))

    section('持久化：损坏索引登记 failed 并被执行跳过')
    val_meta = repo2.find_meta(s2, ds_id, 'value')
    # 先按当前已保存数据重建，使版本对齐 edited，再人为损坏条目，才能走到完整性校验。
    rebuild(repo2, s2, val_meta['id'], edited)
    val_meta = repo2.find_meta(s2, ds_id, 'value')
    repo2.tamper_entry(val_meta['id'])
    corrupt_events = []
    edited['_on_corrupt'] = lambda meta_id, msg: corrupt_events.append((meta_id, msg))
    catalog_safe = store.load_catalog(repo2, s2, edited)
    edited.pop('_on_corrupt', None)
    check('损坏回调被触发并给出原因', bool(corrupt_events) and corrupt_events[0][0] == val_meta['id'])
    check('损坏索引不进入目录', all(c['index_id'] != val_meta['id'] for c in catalog_safe))
    # 生产由 indexes.open_corrupt 在独立事务登记；这里在同会话复现 mark_failed 状态机。
    store.mark_failed(repo2, s2, val_meta['id'], corrupt_events[0][1])
    check('损坏索引状态登记为 failed', repo2.get_meta(s2, val_meta['id'])['status'] == 'failed')
    check('失败原因已记录', bool(repo2.get_meta(s2, val_meta['id'])['error']))
    # 标记 failed 后再次装载仍跳过，且不会重复改写。
    catalog_again = store.load_catalog(repo2, s2, dict(edited, _on_corrupt=lambda *a: None))
    check('failed 索引持续不参与执行', all(c['index_id'] != val_meta['id'] for c in catalog_again))

    # ------------------------------------------------------------------ #
    section('执行：结果与全扫模式逐字节一致，访问量下降')
    clean = SqliteRepo(os.path.join(tmp, 'clean.sqlite'))
    sc = clean.session()
    d2 = make_dataset(n=600)
    d2_id = clean.save_dataset(d2)
    d2['id'] = d2_id
    built = {}
    for flds in (['sensor_id'], ['value'], ['sensor_id', 'value']):
        m = clean.create_meta(sc, d2_id, tuple(flds))
        sc.commit()
        ok, m = store.build_and_publish(clean, sc, m['id'])
        assert ok
        built[','.join(flds)] = m['id']
    full_catalog = store.load_catalog(clean, sc, d2)
    check('三个索引全部进入目录', len(full_catalog) == 3)

    matrix = [('sensor_id 等值', STEPS_EQ), ('value 范围两段', STEPS_RANGE),
              ('组合等值+范围', STEPS_COMPOSITE), ('低选择性', STEPS_LOW_SELECTIVITY),
              ('过滤连接分组全管线', STEPS_FULL_PIPELINE)]
    for label, steps in matrix:
        base = operators.execute(d2, steps, None)
        indexed = operators.execute(d2, steps, full_catalog)
        check('[%s] 输出行一致' % label, indexed['output'] == base['output'])
        check('[%s] checksum 一致' % label, indexed['checksum'] == base['checksum'])
        check('[%s] 逐算子 checksum 一致' % label,
              [t['checksum'] for t in indexed['operators']] ==
              [t['checksum'] for t in base['operators']])
        check('[%s] 输出行数一致' % label, indexed['output_rows'] == base['output_rows'])
        paths = [a['access_path'] for a in indexed['access_paths']]
        print('      访问路径:', paths,
              'row_visits 全扫=%d 索引=%d' % (base['row_visits'], indexed['row_visits']))
        if label in ('sensor_id 等值', 'value 范围两段', '组合等值+范围'):
            check('[%s] 高选择性实际走索引且访问量明显减少' % label,
                  'index_lookup' in paths and indexed['row_visits'] < base['row_visits'])
            ap = next(a['detail'] for a in indexed['access_paths']
                      if a['access_path'] == 'index_lookup')
            check('[%s] 剖面含选择依据与访问量' % label,
                  ap.get('span_entries') is not None and 'reason' in ap and
                  'estimated_visits' in ap and 'full_scan_visits' in ap)
            check('[%s] 回表候选不少于保留行（超集+复核）' % label,
                  ap['fetched_rows'] >= ap['kept_rows'])
        if label == '低选择性':
            check('[%s] 保留全扫且访问量与基线相同' % label,
                  paths == ['full_scan'] and indexed['row_visits'] == base['row_visits'])
        if label == '过滤连接分组全管线':
            check('[%s] 连接前过滤可用索引，连接后字段回退全扫' % label,
                  paths[0] == 'index_lookup' and paths[-1] == 'full_scan')
            check('[%s] 分组结果六位精度未变' % label,
                  all(abs(r['avg'] - round(r['avg'], 6)) == 0 for r in indexed['output']))

    section('执行：旧快照执行、损坏回退与引擎内兜底')
    old_snapshot = dict(d2)
    changed = dict(d2, readings=d2['readings'][:40])
    clean.update_readings(d2_id, changed['readings'])
    for index_id in built.values():
        rebuild(clean, sc, index_id, changed)
    check('旧快照版本不符，目录为空', store.load_catalog(clean, sc, old_snapshot) == [])
    check('新快照目录含三索引', len(store.load_catalog(clean, sc, changed)) == 3)
    r_old = operators.execute(old_snapshot, STEPS_EQ, [])
    r_old_scan = operators.execute(old_snapshot, STEPS_EQ, None)
    check('旧快照全扫执行结果与基线一致',
          r_old['output'] == r_old_scan['output'] and
          r_old['checksum'] == r_old_scan['checksum'])
    # 恢复数据并重建，再损坏 sensor_id 索引
    clean.update_readings(d2_id, d2['readings'])
    for index_id in built.values():
        rebuild(clean, sc, index_id, d2)
    clean.tamper_entry(built['sensor_id'])
    cat_after_tamper = store.load_catalog(clean, sc, d2)
    check('损坏后只剩完好索引',
          sorted(c['index_id'] for c in cat_after_tamper) ==
          sorted([built['value'], built['sensor_id,value']]))
    r_fb = operators.execute(d2, STEPS_EQ, cat_after_tamper)
    check('损坏回退后结果仍与全扫一致',
          r_fb['output'] == r_old_scan['output'] and
          r_fb['checksum'] == r_old_scan['checksum'])
    # 引擎内兜底：结构损坏的条目直接触发异常分支，仍安全回退全扫
    malformed = [{'index_id': 999, 'fields': ['sensor_id'],
                  'entries': [('bad-tuple',)], 'entry_count': 1, 'checksum': 'x'}]
    r_rt = operators.execute(d2, STEPS_EQ, malformed)
    check('引擎运行期异常安全回退全扫且结果正确',
          r_rt['output'] == r_old_scan['output'] and
          r_rt['access_paths'][0]['access_path'] == 'full_scan')

    section('管理：删除索引连带清理条目')
    before_meta = len(clean.list_meta(sc))
    clean.delete_meta(sc, built['value'])
    sc.commit()
    check('删除后元数据与条目一并移除',
          len(clean.list_meta(sc)) == before_meta - 1 and
          clean.count_entries(sc, built['value'], 'active') == 0)

    print('\n全部通过：%d 项断言' % PASS)


if __name__ == '__main__':
    main()
