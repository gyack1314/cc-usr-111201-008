"""统计观察序列的原始异常计数，不进行序贯判断。"""
from collections import defaultdict
def summarize(value):
    streams=value['streams'];seen={};stats=defaultdict(lambda:[0,0])
    for row in value['events']:
        key=(row['stream_id'],row['sequence']);x=row['outcome']
        if key[0] not in streams or type(key[1]) is not int or key[1]<1 or type(x) is not int or x not in (0,1):raise ValueError('事件身份或结果错误')
        if key in seen and seen[key]!=x:raise ValueError('重复事件内容冲突')
        if key not in seen:stats[key[0]][0]+=x;stats[key[0]][1]+=1
        seen[key]=x
    return {k:{'anomalies':v[0],'received':v[1]} for k,v in sorted(stats.items())}
