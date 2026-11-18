from sqlalchemy import Column,Integer,String,JSON,DateTime,ForeignKey,UniqueConstraint,func,Text
from src.config.database import Base


class IndexMeta(Base):
 """readings 逻辑表的持久化二级索引元数据。状态机：building -> available / failed。"""
 __tablename__='index_meta'
 id=Column(Integer,primary_key=True)
 dataset_id=Column(Integer,ForeignKey('datasets.id'),nullable=False)
 fields=Column(JSON,nullable=False)
 fields_key=Column(String(60),nullable=False)
 status=Column(String(20),nullable=False,default='building')
 version=Column(String(64),nullable=True)
 entry_count=Column(Integer,nullable=False,default=0)
 checksum=Column(String(64),nullable=True)
 error=Column(Text,nullable=True)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
 __table_args__=(UniqueConstraint('dataset_id','fields_key',name='uq_index_dataset_fields'),)


class IndexEntry(Base):
 """索引条目：key_json 为键元组 JSON（含六位精度 value），pos 为 reading 回表位置。

 半成品条目标记 staged；只有 build 发布事务里才整体翻为 active，
 因此执行期永远读不到半成品。
 """
 __tablename__='index_entries'
 id=Column(Integer,primary_key=True)
 index_id=Column(Integer,ForeignKey('index_meta.id'),nullable=False,index=True)
 phase=Column(String(10),nullable=False,default='staged')
 pos=Column(Integer,nullable=False)
 key_json=Column(Text,nullable=False)
