"""独立重算：由已确认事件重算每条流的全部前缀，用于审计，不复用监测器的增量状态。"""
from .evalue import StreamRuntime, crosses, log_e, sci_decimal


def stream_prefixes(spec, outcomes):
    """outcomes为已确认顺序（sequence 1..n）的0/1列表；返回t=0..n的全部前缀记录。"""
    rt = StreamRuntime(spec)
    recs = []
    k = 0
    log_e_max = 0.0
    first = None
    for t in range(len(outcomes) + 1):
        if t:
            k += outcomes[t - 1]
        le = log_e(rt, t, k)
        log_e_max = max(log_e_max, le)
        if first is None and crosses(spec, rt, t, k):
            first = t
        recs.append({
            "t": t,
            "n": t,
            "k": k,
            "log_e": le,
            "e": sci_decimal(le),
            "log_e_max": log_e_max,
            "e_max": sci_decimal(log_e_max),
            "first_crossing_t": first,
        })
    return recs


def confirmed_prefix(events):
    """独立分组：同一(stream_id,sequence)冲突报错，同内容重传去重；每流只取从1开始的连续前缀。"""
    by_stream = {}
    for ev in events:
        sid, seq, x = ev["stream_id"], ev["sequence"], ev["outcome"]
        seen = by_stream.setdefault(sid, {})
        if seq in seen and seen[seq] != x:
            raise ValueError(f"事件内容冲突: {sid}#{seq}")
        seen[seq] = x
    out = {}
    for sid, seen in by_stream.items():
        outcomes = []
        seq = 1
        while seq in seen:
            outcomes.append(seen[seq])
            seq += 1
        out[sid] = outcomes
    return out


def audit_events(plan, events):
    """对每条流独立重算全部前缀，返回末态、历来最大与首次越界。"""
    prefixes = confirmed_prefix(events)
    streams = {}
    for sid, spec in plan.streams.items():
        recs = stream_prefixes(spec, prefixes.get(sid, []))
        last = recs[-1]
        streams[sid] = {
            "applied": last["n"],
            "anomalies": last["k"],
            "log_e": last["log_e"],
            "log_e_max": last["log_e_max"],
            "first_crossing_t": last["first_crossing_t"],
            "prefixes": recs,
        }
    return {"plan_id": plan.plan_id, "streams": streams}


def compare_with_status(audit, status, tol=1e-12):
    """比对独立重算与监测器当前状态，返回不一致项列表（空为一致）。"""
    bad = []
    for sid, a in audit["streams"].items():
        s = status["streams"].get(sid)
        if s is None:
            bad.append(f"{sid}: 状态中缺失")
            continue
        if a["applied"] != s["applied"] or a["anomalies"] != s["anomalies"]:
            bad.append(f"{sid}: 计数不一致")
        if abs(a["log_e"] - s["log_e"]) > tol or abs(a["log_e_max"] - s["log_e_max"]) > tol:
            bad.append(f"{sid}: e值不一致")
        if a["first_crossing_t"] != s["first_crossing_t"]:
            bad.append(f"{sid}: 首次越界位置不一致")
    return bad
