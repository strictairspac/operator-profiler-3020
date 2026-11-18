# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores immutable JSON input snapshots. PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.

# Schema v2 — readings 持久化二级索引
新增 index_meta / index_entries 两表，由 SQLAlchemy metadata 随 Base.metadata.create_all 建立。
- index_meta：dataset_id + fields_key 唯一；fields 为 ["sensor_id"] / ["value"] / ["sensor_id","value"]；
  状态机 building -> available / failed；version 为建设发布时 readings 规范 JSON 的 SHA-256，
  entry_count/checksum 为条目计数与校验和。
- index_entries：index_id 外键；phase=staged|active（建设半成品仅 staged，发布事务整体翻 active）；
  pos 为 reading 回表位置，key_json 为键元组（value 沿用六位精度）。
执行只读取 active 条目，且要求 index_meta.status=available 且 version 等于受理快照版本；
版本不符跳过、校验失败登记 failed 并回退全扫。
