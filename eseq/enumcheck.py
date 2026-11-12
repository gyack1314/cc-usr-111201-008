"""小样本枚举：在独立Bernoulli(p0)路径下精确核算越界概率是否不超过该流预算。

此枚举是示例性校验，不替代一般条件null（给定历史条件概率不超过p0）下的超鞅保证。
"""
from fractions import Fraction

from .evalue import StreamRuntime, crosses


def exceedance_probability(spec, n, p=None):
    """P(存在0<=t<=n使e_t>=1/alpha)的精确分数值；路径按Bernoulli(p)加权，p缺省为p0。"""
    if type(n) is not int or n < 0:
        raise ValueError("n必须是非负整数")
    p = spec.p0 if p is None else p
    rt = StreamRuntime(spec)
    states = {(0, 0): Fraction(1)}  # (t,k) -> 尚未越界而到达该状态的概率
    exceed = Fraction(0)
    for t in range(n + 1):
        nxt = {}
        for (tt, k), pr in states.items():
            if crosses(spec, rt, t, k):
                exceed += pr
                continue
            if t == n:
                continue
            for dk, w in ((1, p), (0, 1 - p)):
                key = (t + 1, k + dk)
                nxt[key] = nxt.get(key, Fraction(0)) + pr * w
        states = nxt
    return exceed


def check_plan(plan, n):
    """逐流核算越界概率并核对联合预算：各流alpha之和不超过global_alpha。"""
    streams = []
    for sid, spec in sorted(plan.streams.items()):
        pr = exceedance_probability(spec, n)
        streams.append({
            "stream_id": sid,
            "n": n,
            "alpha": str(spec.alpha),
            "exceedance_probability": str(pr),
            "exceedance_value": float(pr),
            "within_budget": pr <= spec.alpha,
        })
    total = sum((s.alpha for s in plan.streams.values()), Fraction(0))
    return {
        "streams": streams,
        "sum_alpha": str(total),
        "global_alpha": str(plan.global_alpha),
        "joint_bound_ok": total <= plan.global_alpha,
        "caveat": "按每流独立Bernoulli(p0)枚举路径概率，仅为示例性校验，不替代一般条件null保证。",
    }
