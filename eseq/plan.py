"""监测计划：解析、校验、冻结与身份。计划开启后不可修改；任何配置变更必须开启新计划。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction

ASSUMPTIONS = (
    "原假设H0：在已知该流历史结果的条件下，每一步异常(1)的条件概率不超过p0，不假设各步独立。"
    "e值是各备择财富（每个q_j内部先连乘似然比）按启动前冻结权重的混合，在H0下为非负超鞅；"
    "由Ville不等式，单流首次越界概率P(存在t使e>=1/alpha_s)不超过alpha_s；"
    "各流预算alpha_s启动前冻结且总和不超过global_alpha，由并合界，全体流在任一时刻的误报概率不超过global_alpha。"
    "e值不是概率，也不是当前异常率；当前比例k/n单独列出。"
    "报警后继续记录但不重置证据、不另领预算；新增流或修改p0/q/alpha必须开启新计划，旧计划保留。"
)

_STREAM_KEYS = {"p0", "q", "mixture", "alpha"}
_PLAN_KEYS = {"global_alpha", "streams", "events"}


def parse_fraction(value, name):
    """只接受精确分数串（'a/b'或'a'）或整数，拒绝浮点与布尔。"""
    if isinstance(value, bool):
        raise ValueError(f"{name}不能接受布尔值")
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, str):
        parts = value.strip().split("/")
        try:
            if len(parts) == 1:
                return Fraction(int(parts[0]))
            if len(parts) == 2:
                return Fraction(int(parts[0]), int(parts[1]))
        except ValueError:
            pass
    raise ValueError(f"{name}必须是精确分数串或整数: {value!r}")


@dataclass(frozen=True)
class StreamSpec:
    """单条流冻结的统计配置。"""

    stream_id: str
    p0: Fraction
    qs: tuple
    weights: tuple
    alpha: Fraction

    def config_dict(self):
        return {
            "p0": str(self.p0),
            "q": [str(q) for q in self.qs],
            "mixture": [str(w) for w in self.weights],
            "alpha": str(self.alpha),
        }


@dataclass(frozen=True)
class Plan:
    """一份已开启的监测计划；streams为{stream_id: StreamSpec}。"""

    plan_id: str
    global_alpha: Fraction
    streams: dict
    opened_at: str = ""

    def summary(self):
        return {
            "plan_id": self.plan_id,
            "opened_at": self.opened_at,
            "global_alpha": str(self.global_alpha),
            "sum_alpha": str(sum((s.alpha for s in self.streams.values()), Fraction(0))),
            "assumptions": ASSUMPTIONS,
            "streams": {
                sid: {**spec.config_dict(), "threshold": str(Fraction(1, 1) / spec.alpha)}
                for sid, spec in sorted(self.streams.items())
            },
        }


def parse_config(config):
    """校验并返回(global_alpha, {stream_id: StreamSpec})；允许并忽略顶层events键。"""
    if not isinstance(config, dict):
        raise ValueError("计划配置必须是对象")
    unknown = set(config) - _PLAN_KEYS
    if unknown:
        raise ValueError(f"计划配置含未知键: {sorted(unknown)}")
    global_alpha = parse_fraction(config["global_alpha"], "global_alpha")
    if not 0 < global_alpha:
        raise ValueError("global_alpha必须为正")
    raw_streams = config["streams"]
    if not isinstance(raw_streams, dict) or not raw_streams:
        raise ValueError("streams必须是非空对象")
    streams = {}
    for sid, sc in raw_streams.items():
        if not isinstance(sid, str) or not sid:
            raise ValueError("stream_id必须是非空字符串")
        if not isinstance(sc, dict) or set(sc) != _STREAM_KEYS:
            raise ValueError(f"流{sid}的配置键必须是{sorted(_STREAM_KEYS)}")
        p0 = parse_fraction(sc["p0"], f"{sid}.p0")
        if not 0 < p0 < 1:
            raise ValueError(f"流{sid}的p0必须在(0,1)内")
        qs_raw, mix_raw = sc["q"], sc["mixture"]
        if not isinstance(qs_raw, list) or not isinstance(mix_raw, list) or not qs_raw or len(qs_raw) != len(mix_raw):
            raise ValueError(f"流{sid}的q与mixture必须是等长非空数组")
        qs = tuple(parse_fraction(q, f"{sid}.q") for q in qs_raw)
        for q in qs:
            if not p0 < q < 1:
                raise ValueError(f"流{sid}的每个q必须满足p0<q<1")
        weights = tuple(parse_fraction(w, f"{sid}.mixture") for w in mix_raw)
        if any(w <= 0 for w in weights) or sum(weights) != 1:
            raise ValueError(f"流{sid}的混合权重必须为正且和为1")
        alpha = parse_fraction(sc["alpha"], f"{sid}.alpha")
        if not 0 < alpha < 1:
            raise ValueError(f"流{sid}的alpha必须在(0,1)内")
        streams[sid] = StreamSpec(sid, p0, qs, weights, alpha)
    total = sum((s.alpha for s in streams.values()), Fraction(0))
    if total > global_alpha:
        raise ValueError(f"各流alpha之和{total}超过global_alpha {global_alpha}")
    return global_alpha, streams


def canonical_form(config):
    """计划的规范化形态：解析为精确分数后重新序列化，语义等价的写法得到同一身份。"""
    global_alpha, streams = parse_config(config)
    return {
        "global_alpha": str(global_alpha),
        "streams": {sid: streams[sid].config_dict() for sid in sorted(streams)},
    }


def canonical_id(config):
    """计划的冻结身份：规范化配置的SHA-256。事件不属于计划身份。"""
    blob = json.dumps(canonical_form(config), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf8")).hexdigest()


def build_plan(config, opened_at=""):
    global_alpha, streams = parse_config(config)
    return Plan(canonical_id(config), global_alpha, streams, opened_at)
