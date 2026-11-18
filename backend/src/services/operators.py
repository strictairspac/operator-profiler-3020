"""A bounded teaching execution engine; this does not replace PostgreSQL execution.

filter 步骤支持两条访问路径：
- full_scan：原教学内核行为，逐行谓词判断；
- index_lookup：基于 readings 的持久化二级索引（sensor_id/value，单列或组合），
  bisect 定位键范围、按位置回表，仅当估算访问量严格小于全扫才启用。
两条路径输出行顺序完全一致（回表位置升序即原 reading 顺序），checksum 与结果相同；
索引版本不符或条目损坏时由调用方保证不进入目录，引擎内再做异常兜底回退。
"""
from collections import defaultdict
from decimal import Decimal
import time,json,hashlib,math
from src.constants.enums import OPERATORS
from src.services.indexing import core
MAX_ROWS=10000
READING_FIELDS={'sensor_id','value','recorded_at'}
SENSOR_FIELDS={'sensor_id','zone','device_type'}
def validate_steps(steps):
 if not isinstance(steps,list) or not 1<=len(steps)<=6:raise ValueError('计划须包含1至6个算子')
 fields=set();grouped=False
 for i,step in enumerate(steps):
  if not isinstance(step,dict) or step.get('op') not in OPERATORS:raise ValueError('非法算子；仅允许scan/filter/hash_join/group')
  op=step['op']
  if i==0 and op!='scan':raise ValueError('第一步必须是scan')
  if op=='scan':
   if i!=0 or step!={'op':'scan','source':'readings'}:raise ValueError('scan只能在首步读取readings')
   fields=READING_FIELDS.copy()
  elif op=='filter':
   if set(step)!={'op','field','compare','value'} or not isinstance(step['field'],str) or step['field'] not in fields or step['compare'] not in ('eq','gte','lte'):raise ValueError('filter字段或比较方式无效')
   if step['compare']!='eq' and (step['field'] not in ('sensor_id','value','count','sum','avg') or not isinstance(step['value'],(int,float)) or not math.isfinite(step['value'])):raise ValueError('范围筛选只接受数值字段和有限数值')
   if not isinstance(step['value'],(str,int,float)) or isinstance(step['value'],bool) or (isinstance(step['value'],float) and not math.isfinite(step['value'])):raise ValueError('filter值只接受文本或数值')
  elif op=='hash_join':
   if grouped or not READING_FIELDS.issubset(fields) or step!={'op':'hash_join','right':'sensors','left_key':'sensor_id','right_key':'sensor_id'}:raise ValueError('hash_join仅支持分组前readings与sensors按sensor_id内连接')
   if 'zone' in fields:raise ValueError('一个计划只允许一次连接')
   fields|=SENSOR_FIELDS
  elif op=='group':
   if set(step)!={'op','keys','measure','aggregate'} or grouped:raise ValueError('仅允许一次group，参数为keys/measure/aggregate')
   keys=step['keys']
   if not isinstance(keys,list) or not 1<=len(keys)<=2 or any(not isinstance(k,str) for k in keys) or len(set(keys))!=len(keys) or any(k not in fields for k in keys):raise ValueError('分组字段无效')
   if step['measure']!='value' or step['aggregate'] not in ('count','sum','avg'):raise ValueError('group仅允许value的count/sum/avg')
   fields=set(keys)|{step['aggregate']};grouped=True
 return steps

def checksum(rows):return hashlib.sha256(json.dumps(rows,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()

def _match(field,cmp,value,row):
 v=row[field]
 if cmp=='eq':return v==value
 if not isinstance(v,(int,float)):raise ValueError('范围比较遇到非数值')
 return v>=value if cmp=='gte' else v<=value

def execute(dataset,steps,index_catalog=None):
 validate_steps(steps);rows=[];trace=[];total_started=time.perf_counter();reads=0
 # 仅在仍处于 readings 行集（连接/分组之前）时索引才可能生效。
 indexable=True
 # 当前行集：每项为 (原始 reading 位置, 行 dict)，过滤时成对收缩，
 # 原始位置始终可用于索引候选求交，不会因行集变短而越界。
 live=[(i,dict(r)) for i,r in enumerate(dataset.get('readings',[]))]
 # 已受理的 readings 前导 filter 谓词（供组合索引选择）。
 predicates=[]
 access_paths=[]
 for step in steps:
  before=len(rows);started=time.perf_counter();detail={}
  if step['op']=='scan':
   live=[(i,dict(r)) for i,r in enumerate(dataset['readings'])]
   rows=[r for _,r in live];reads+=len(rows)
   detail={'source':'readings','scanned_rows':len(rows)}
  elif step['op']=='filter':
   field,cmp,value=step['field'],step['compare'],step['value']
   path='full_scan';plan_detail={'access_path':'full_scan','reason':'默认全扫路径'}
   use_index=indexable and bool(index_catalog) and field in core.SUPPORTED_FIELDS
   index_handled=False
   if use_index:
    pending=predicates+[{'field':field,'compare':cmp,'value':value}]
    try:
     chosen,candidates,reason=core.plan_index(index_catalog,pending,len(live))
    except Exception as exc:
     # 规划期触碰损坏条目：本步直接全扫，结果口径不变。
     chosen,candidates,reason=None,[],('索引规划异常，回退全扫：%s'%exc)
    plan_detail={'access_path':'full_scan','reason':reason,'index_candidates':candidates}
    if chosen:
     index=next(i for i in index_catalog if i['index_id']==chosen['index_id'])
     try:
      matched_predicates=[p for p in pending if p['field'] in index['fields']]
      sought=core.seek(index['entries'],index['fields'],matched_predicates)
      if sought is None:
       raise core.IndexCorrupt('索引不兼容该谓词类型')
      candidate_positions,span=sought
      alive_positions={p for p,_ in live}
      # 候选按键序返回，与存活集合求交后按原始位置升序回表，
      # 输出顺序与全扫（reading 顺序）逐字节一致。
      picked=[(p,r) for p,r in live if p in set(candidate_positions) and _match(field,cmp,value,r)]
      fetched=sum(1 for p in candidate_positions if p in alive_positions)
      live=picked;rows=[r for _,r in live]
      reads+=span+fetched
      path='index_lookup';index_handled=True
      plan_detail={'access_path':'index_lookup','index_id':index['index_id'],
       'index_fields':index['fields'],'predicates':matched_predicates,
       'span_entries':span,'fetched_rows':fetched,'kept_rows':len(live),
       'estimated_visits':chosen['estimated_visits'],'full_scan_visits':before,
       'reason':reason,'index_candidates':candidates}
     except Exception as exc:
      # 运行期任何索引异常都安全回退本步全扫，结果口径不变。
      plan_detail={'access_path':'full_scan','reason':'索引检索异常，回退全扫：%s'%exc,'index_candidates':candidates}
   if not index_handled:
    live=[(p,r) for p,r in live if _match(field,cmp,value,r)]
    rows=[r for _,r in live];reads+=before
    if use_index:
     plan_detail['access_path']='full_scan'
   if indexable:
    predicates.append({'field':field,'compare':cmp,'value':value})
   detail={'predicate':step,'removed_rows':before-len(rows),'access':plan_detail}
   access_paths.append({'step':len(trace),'op':'filter','field':field,'compare':cmp,
                        'value':value,'access_path':path,'detail':plan_detail})
  elif step['op']=='hash_join':
   buckets=defaultdict(list)
   for sensor in dataset['sensors']:buckets[sensor['sensor_id']].append(sensor)
   joined=[];probes=0
   for row in rows:
    probes+=1
    for sensor in buckets.get(row['sensor_id'],[]):
     joined.append({**row,**sensor})
     if len(joined)>MAX_ROWS:raise ValueError('连接结果超过10000行，请缩小输入；未保存部分结果')
   reads+=len(rows)+len(dataset['sensors']);rows=joined
   # 连接后行已脱离 reading 位置，以枚举序号重建 live；indexable=False 后索引不再使用。
   live=list(enumerate(rows))
   indexable=False;predicates=[]
   detail={'build_rows':len(dataset['sensors']),'hash_buckets':len(buckets),'probe_rows':probes,'semantics':'inner join; duplicate right keys retain multiplicity'}
  elif step['op']=='group':
   grouped=defaultdict(list);keys=step['keys'];agg=step['aggregate']
   for row in rows:grouped[tuple(row[k] for k in keys)].append(Decimal(str(row['value'])))
   reads+=len(rows);rows=[]
   for key,values in grouped.items():
    value=len(values) if agg=='count' else float(sum(values)) if agg=='sum' else float(sum(values)/len(values))
    rows.append({**dict(zip(keys,key)),agg:round(value,6) if isinstance(value,float) else value})
   rows.sort(key=lambda r:json.dumps(r,sort_keys=True,ensure_ascii=False));detail={'groups':len(rows),'aggregate':agg,'accumulator':'decimal from source string; rounded 6 decimals'}
   indexable=False;live=[];predicates=[]
  trace.append({'op':step['op'],'input_rows':before,'output_rows':len(rows),'duration_ms':round((time.perf_counter()-started)*1000,4),'detail':detail,'checksum':checksum(rows),'preview':rows[:5]})
 return {'operators':trace,'output':rows,'output_rows':len(rows),'checksum':checksum(rows),'row_visits':reads,'duration_ms':round((time.perf_counter()-total_started)*1000,4),'limits':{'max_readings':2000,'max_sensors':100,'max_join_rows':10000,'max_stages':6},'model':'Python teaching scan/filter/hash join/group engine; PostgreSQL persists business metadata only','access_paths':access_paths}
