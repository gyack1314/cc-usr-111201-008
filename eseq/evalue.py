"""e值数学：log域混合财富、精确边界判定、十进制科学计数表示。

定义：E_t = sum_j w_j * prod_{s<=t} LR_j(x_s)，LR_j(1)=q_j/p0，LR_j(0)=(1-q_j)/(1-p0)。
先在每个j内部连乘再按权重混合，不是每步混合因子的连乘。空前缀E_0=1。
"""
import math
from fractions import Fraction

# log域判定带：|log_e - log阈值|落入带内时改用精确分数判定，避免log舍入漏掉恰等阈值。
BAND = 1e-9


class StreamRuntime:
    """由StreamSpec预计算的log系数。"""

    __slots__ = ("log_a", "log_b", "log_w", "log_threshold")

    def __init__(self, spec):
        self.log_a = [math.log(q / spec.p0) for q in spec.qs]
        self.log_b = [math.log((1 - q) / (1 - spec.p0)) for q in spec.qs]
        self.log_w = [math.log(w) for w in spec.weights]
        self.log_threshold = -math.log(spec.alpha)


def log_e(rt, n, k):
    """第n步、累计k次异常时的log e值。log域更新，长链不下溢不上溢。"""
    terms = [lw + k * a + (n - k) * b for lw, a, b in zip(rt.log_w, rt.log_a, rt.log_b)]
    m = max(terms)
    return m + math.log(sum(math.exp(x - m) for x in terms))


def exact_e(spec, n, k):
    """同一量的精确分数值（配置全为有理数，故e值精确有理）。仅用于边界判定与校验。"""
    total = Fraction(0)
    for w, q in zip(spec.weights, spec.qs):
        total += w * (q / spec.p0) ** k * ((1 - q) / (1 - spec.p0)) ** (n - k)
    return total


def crosses(spec, rt, n, k):
    """是否e >= 1/alpha（恰等计入）。log域粗判，带内用精确分数定夺。"""
    le = log_e(rt, n, k)
    lt = rt.log_threshold
    band = BAND * max(1.0, abs(le), abs(lt))
    if le > lt + band:
        return True
    if le < lt - band:
        return False
    return exact_e(spec, n, k) >= Fraction(1, 1) / spec.alpha


def sci_decimal(log_e_value):
    """由log值给出十进制科学计数字符串；极大极小数不输出Inf/NaN。"""
    if log_e_value == 0.0:
        return "1"
    if -700.0 < log_e_value < 700.0:
        e = math.exp(log_e_value)
        if 1e-4 <= e < 1e6:
            return f"{e:.6g}"
        return f"{e:.6e}"
    log10 = log_e_value / math.log(10.0)
    exp10 = math.floor(log10)
    mantissa = f"{10.0 ** (log10 - exp10):.6f}"
    if mantissa.startswith("10."):
        return f"1.000000e{exp10 + 1:+d}"
    return f"{mantissa}e{exp10:+d}"
