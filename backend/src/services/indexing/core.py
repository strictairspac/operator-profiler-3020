"""持久化索引的纯 Python 内核：只依赖标准库，不导入 SQLAlchemy/Redis。

设计口径：
- 索引对象是 readings 逻辑表的二级索引，键字段仅支持 sensor_id / value，
  含单列与 (sensor_id, value) 组合索引；等值、范围与组合谓词统一转为键上下界。
- 每条 reading 产生一个索引条目 (key..., position)，重复记录全部保留（不去重）。
- value 键沿用教学内核六位精度（round 6）；sensor_id 为整数键。
- 条目按 (键, 位置) 排序，bisect 定位；回表位置排序后与原 list 过滤顺序一致，
  因此输出行、checksum 与全扫模式完全相同。
- 数据集版本 = readings 规范 JSON 的 SHA-256；执行按受理快照选索引，版本不符即不入选。
"""
from bisect import bisect_left, bisect_right
import hashlib
import json

SUPPORTED_FIELDS = ('sensor_id', 'value')
NEG_INF = float('-inf')
POS_INF = float('inf')


class IndexCorrupt(RuntimeError):
    """索引条目与其声明的校验信息不一致（损坏）。"""


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def dataset_checksum(dataset):
    """readings 的内容版本（顺序敏感），作为索引版本键。"""
    return hashlib.sha256(canonical_json(dataset.get('readings', [])).encode('utf-8')).hexdigest()


def normalize_fields(fields):
    """规范化字段组合：去重、限支持字段、固定 sensor_id 在 value 前的顺序。"""
    if not isinstance(fields, (list, tuple)) or not fields or len(fields) > 2:
        raise ValueError('索引字段须为 1 至 2 个支持字段')
    if any(f not in SUPPORTED_FIELDS for f in fields) or len(set(fields)) != len(fields):
        raise ValueError('仅支持 sensor_id、value 的单列或组合索引')
    return tuple(f for f in SUPPORTED_FIELDS if f in fields)


def fields_key(fields):
    return ','.join(normalize_fields(fields))


def _key_of(fields, row):
    parts = []
    for f in fields:
        if f == 'value':
            parts.append(round(float(row[f]), 6))
        else:
            parts.append(int(row[f]))
    return tuple(parts)


def build_entries(dataset, fields):
    """全量构建条目：每条 reading 一条，重复键保留，按 (键, 位置) 排序。"""
    fields = normalize_fields(fields)
    entries = []
    for position, row in enumerate(dataset.get('readings', [])):
        entries.append(_key_of(fields, row) + (position,))
    entries.sort(key=lambda e: e[:-1] + (e[-1],))
    return entries


def entries_checksum(entries):
    return hashlib.sha256(canonical_json([list(e) for e in entries]).encode('utf-8')).hexdigest()


def verify_index(index, dataset):
    """执行前完整性校验：声明计数/校验和、条目有序性、位置与键全部核对。

    index: dict，含 fields / entries / entry_count / checksum。
    任何不一致抛 IndexCorrupt；正常返回 None。
    """
    fields = normalize_fields(index['fields'])
    entries = [tuple(e) for e in index['entries']]
    n = len(dataset.get('readings', []))
    if len(entries) != index.get('entry_count'):
        raise IndexCorrupt('索引条目计数与实际不符')
    if entries_checksum(entries) != index.get('checksum'):
        raise IndexCorrupt('索引校验和不匹配，条目可能已损坏')
    if entries != sorted(entries, key=lambda e: e[:-1] + (e[-1],)):
        raise IndexCorrupt('索引条目未按键有序排列')
    seen = set()
    for entry in entries:
        position = entry[-1]
        if not isinstance(position, int) or not 0 <= position < n:
            raise IndexCorrupt('索引存在越界回表位置')
        if position in seen:
            raise IndexCorrupt('索引存在重复回表位置')
        seen.add(position)
        if entry[:-1] != _key_of(fields, dataset['readings'][position]):
            raise IndexCorrupt('索引键与源数据不一致')
    if len(seen) != n:
        raise IndexCorrupt('索引未覆盖全部采样记录')


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _field_bounds(field, predicates):
    """把同一字段上的全部谓词折叠为 (lo, hi) 闭区间；矛盾返回 None。"""
    lo, hi = NEG_INF, POS_INF
    for predicate in predicates:
        cmp, value = predicate['compare'], predicate['value']
        if not _is_number(value):
            return None  # 数值索引不服务文本等值，交回全扫
        if field == 'value':
            value = round(float(value), 6)
        else:
            value = int(value)
        if cmp == 'eq':
            cmp_lo = cmp_hi = value
        elif cmp == 'gte':
            cmp_lo, cmp_hi = value, POS_INF
        else:
            cmp_lo, cmp_hi = NEG_INF, value
        if cmp_lo > lo:
            lo = cmp_lo
        if cmp_hi < hi:
            hi = cmp_hi
    if lo > hi:
        return NEG_INF, POS_INF, True
    return lo, hi, False


def seek(entries, fields, predicates):
    """返回 (候选位置有序列表, 跨越条目数 span)；矛盾谓词返回 ([], 0)。"""
    fields = normalize_fields(fields)
    lo, hi = [], []
    for f in fields:
        matched = [p for p in predicates if p['field'] == f]
        if not matched:
            lo.append(NEG_INF)
            hi.append(POS_INF)
            continue
        bounds = _field_bounds(f, matched)
        if bounds is None:
            return None  # 类型不兼容，调用方应回退全扫
        field_lo, field_hi, contradictory = bounds
        if contradictory:
            return [], 0
        lo.append(field_lo)
        hi.append(field_hi)
    lo_tuple, hi_tuple = tuple(lo), tuple(hi)
    left = bisect_left(entries, lo_tuple + (NEG_INF,))
    right = bisect_right(entries, hi_tuple + (POS_INF,))
    span = right - left
    return [e[-1] for e in entries[left:right]], span


def plan_index(catalog, predicates, current_count):
    """按已受理的前导谓词集合选择访问路径。

    catalog: 已通过版本与状态筛选、且完整性已校验的索引字典列表。
    返回 (chosen_index 或 None, 候选说明列表, 理由)。
    仅当索引估算访问量严格小于全扫访问量才选索引（低选择性不强用）。
    """
    predicate_fields = {p['field'] for p in predicates}
    full_scan_visits = current_count
    candidates = []
    chosen = None
    for index in catalog:
        fields = normalize_fields(index['fields'])
        if not set(fields).issubset(predicate_fields):
            continue  # 索引首列无前导谓词，无法使用
        matched = [p for p in predicates if p['field'] in fields]
        sought = seek(index['entries'], fields, matched)
        if sought is None:
            continue
        positions, span = sought
        # 估算回表行数：跨越条目与当前行集取小，访问量 = 跨越 + 回表
        estimated_fetch = min(span, current_count)
        estimated_visits = span + estimated_fetch
        candidate = {
            'index_id': index['index_id'],
            'fields': list(fields),
            'span_entries': span,
            'estimated_fetch_rows': estimated_fetch,
            'estimated_visits': estimated_visits,
        }
        candidates.append(candidate)
        if estimated_visits < full_scan_visits and (
            chosen is None or estimated_visits < chosen['estimated_visits']
        ):
            chosen = candidate
    if chosen:
        reason = '估算索引访问量 %d 小于全扫 %d，选择索引 %s' % (
            chosen['estimated_visits'], full_scan_visits, fields_key(chosen['fields']))
    elif candidates:
        reason = '候选索引估算访问量均不小于全扫 %d，低选择性下保留全扫' % full_scan_visits
    else:
        reason = '无覆盖前导谓词的可用索引，使用全扫'
    return chosen, candidates, reason
