"""四个必需场景的演示：长链下溢、恰等阈值、缺口补齐、进程重启。"""
import math
import tempfile
from decimal import Decimal, localcontext
from fractions import Fraction

from .evalue import StreamRuntime, exact_e, log_e, sci_decimal
from .monitor import Monitor
from .plan import build_plan
from .store import Store

_CHAIN_CONFIG = {
    "global_alpha": "1/1000",
    "streams": {"chain": {"p0": "1/4", "q": ["5/8"], "mixture": ["1"], "alpha": "1/1000"}},
}

_THRESHOLD_CONFIG = {
    "global_alpha": "1/4",
    "streams": {"one": {"p0": "1/4", "q": ["1/2"], "mixture": ["1"], "alpha": "1/4"}},
}

_MIX_CONFIG = {
    "global_alpha": "1/10",
    "streams": {"s": {"p0": "1/5", "q": ["2/5", "4/5"], "mixture": ["1/2", "1/2"], "alpha": "1/20"}},
}


def demo_underflow(n=3000):
    """长链下溢：朴素float64连乘约千步后下溢为0（反向则上溢为inf），log域保持有限精确。

    失败因子(1-q)/(1-p0)=1/2，成功因子q/p0=5/2。
    """
    plan = build_plan(_CHAIN_CONFIG)
    spec = plan.streams["chain"]
    rt = StreamRuntime(spec)
    naive_down = 1.0
    for _ in range(n):
        naive_down *= 0.5
    naive_up = 1.0
    for _ in range(n):
        naive_up *= 2.5
    le_down = log_e(rt, n, 0)
    le_up = log_e(rt, n, n)
    with localcontext() as ctx:
        ctx.prec = 60
        ref = float((Decimal(1) / Decimal(2) ** n).ln())
    return {
        "n": n,
        "naive_float64_all_fail": naive_down,
        "naive_float64_all_success": "inf" if math.isinf(naive_up) else naive_up,
        "log_e_all_fail": le_down,
        "e_all_fail": sci_decimal(le_down),
        "log_e_all_success": le_up,
        "e_all_success": sci_decimal(le_up),
        "decimal_crosscheck_log_e": ref,
        "crosscheck_rel_err": abs(ref - le_down) / abs(ref),
        "exact_fraction_check": exact_e(spec, 10, 0) == Fraction(1, 2) ** 10,
    }


def demo_exact_threshold():
    """恰等阈值：两个成功后e精确等于1/alpha=4，恰等计入越界，必须报警。"""
    mon = Monitor(build_plan(_THRESHOLD_CONFIG))
    mon.ingest({"stream_id": "one", "sequence": 1, "outcome": 1})
    res = mon.ingest({"stream_id": "one", "sequence": 2, "outcome": 1})
    spec = mon.plan.streams["one"]
    exact = exact_e(spec, 2, 2)
    return {
        "exact_e_after_two_successes": str(exact),
        "threshold": str(Fraction(1, 1) / spec.alpha),
        "exactly_equal": exact == Fraction(1, 1) / spec.alpha,
        "alarm_fired_at_t2": bool(res["alarms"]) and res["alarms"][0]["t"] == 2,
        "first_crossing_t": mon.states["one"].first_crossing_t,
    }


def demo_gap_fill():
    """缺口补齐：先到的seq=3被暂存，seq=1,2补齐后连续处理；终态与顺序交付一致。"""
    outcomes = [1, 1, 0]
    late = Monitor(build_plan(_MIX_CONFIG))
    late.ingest({"stream_id": "s", "sequence": 3, "outcome": outcomes[2]})
    applied_while_gap = late.status()["streams"]["s"]["applied"]
    late.ingest({"stream_id": "s", "sequence": 1, "outcome": outcomes[0]})
    res = late.ingest({"stream_id": "s", "sequence": 2, "outcome": outcomes[1]})
    in_order = Monitor(build_plan(_MIX_CONFIG))
    for i, x in enumerate(outcomes, 1):
        in_order.ingest({"stream_id": "s", "sequence": i, "outcome": x})
    a = late.status()["streams"]["s"]
    b = in_order.status()["streams"]["s"]
    keys = ("applied", "anomalies", "log_e", "log_e_max", "first_crossing_t")
    return {
        "applied_while_gap": applied_while_gap,
        "drained_on_fill": [step["t"] for step in res["steps"]],
        "matches_in_order": all(a[k] == b[k] for k in keys),
    }


def demo_restart():
    """进程重启：状态由日志重放重建，报警回执重推导校验；重启后续传一致。"""
    with tempfile.TemporaryDirectory() as d:
        store = Store(d)
        plan, is_new = store.open_plan(_MIX_CONFIG)
        mon = store.monitor(plan.plan_id)
        for i, x in enumerate([1, 1, 0, 1], 1):
            mon.ingest({"stream_id": "s", "sequence": i, "outcome": x})
        before = mon.status()
        reopened = store.monitor(plan.plan_id)  # 模拟进程退出后重开
        replayed_equal = reopened.status() == before
        reopened.ingest({"stream_id": "s", "sequence": 5, "outcome": 0})
        again = store.monitor(plan.plan_id)
        return {
            "plan_is_new": is_new,
            "reopened_equal": replayed_equal,
            "continued_equal": again.status() == reopened.status(),
            "applied_after_restart": again.status()["streams"]["s"]["applied"],
        }


def run_all():
    return {
        "underflow": demo_underflow(),
        "exact_threshold": demo_exact_threshold(),
        "gap_fill": demo_gap_fill(),
        "restart": demo_restart(),
    }
