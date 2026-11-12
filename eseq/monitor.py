"""内存监测器：事件校验、幂等、流内缺口缓冲、log域证据更新与首次越界报警。"""
from fractions import Fraction

from .evalue import StreamRuntime, crosses, log_e, sci_decimal
from .plan import ASSUMPTIONS


class InvalidEvent(ValueError):
    pass


class UnknownStream(InvalidEvent):
    pass


class EventConflict(ValueError):
    """同一(stream_id, sequence)出现不同结果。"""


_EVENT_KEYS = {"event_id", "stream_id", "sequence", "outcome"}


def validate_event(plan, event):
    """事件身份=(stream_id, sequence)；event_id仅为来源追踪元数据，不取代此二元身份。"""
    if not isinstance(event, dict) or not set(event) <= _EVENT_KEYS:
        raise InvalidEvent("事件必须是对象且只含event_id/stream_id/sequence/outcome")
    sid = event.get("stream_id")
    seq = event.get("sequence")
    x = event.get("outcome")
    eid = event.get("event_id")
    if not isinstance(sid, str) or sid not in plan.streams:
        raise UnknownStream(f"未知stream_id: {sid!r}")
    if type(seq) is not int or seq < 1:
        raise InvalidEvent("sequence必须从1开始的整数")
    if type(x) is not int or x not in (0, 1):
        raise InvalidEvent("outcome必须是0或1")
    if eid is not None and not isinstance(eid, str):
        raise InvalidEvent("event_id必须是字符串")
    ev = {"stream_id": sid, "sequence": seq, "outcome": x}
    if eid is not None:
        ev["event_id"] = eid
    return ev


class StreamState:
    """单条流的运行时状态：已处理连续前缀、缺口暂存、证据与首次越界。"""

    def __init__(self, spec):
        self.spec = spec
        self.rt = StreamRuntime(spec)
        self.n = 0
        self.k = 0
        self.log_e_max = 0.0  # 历来最大log e，含空前缀e=1
        self.first_crossing_t = None
        self.next_seq = 1
        self.buffer = {}  # 缺口之外的暂存：sequence -> outcome
        self.seen = {}    # 全部已受理事件：sequence -> outcome（含暂存）

    @property
    def alarmed(self):
        return self.first_crossing_t is not None


def _stream_view(state):
    spec = state.spec
    le = log_e(state.rt, state.n, state.k)
    return {
        "p0": str(spec.p0),
        "q": [str(q) for q in spec.qs],
        "mixture": [str(w) for w in spec.weights],
        "alpha": str(spec.alpha),
        "threshold": str(Fraction(1, 1) / spec.alpha),
        "applied": state.n,
        "anomalies": state.k,
        "received": len(state.seen),
        "buffered": len(state.buffer),
        "next_sequence": state.next_seq,
        "proportion": f"{state.k}/{state.n}" if state.n else None,
        "proportion_value": state.k / state.n if state.n else None,
        "log_e": le,
        "e": sci_decimal(le),
        "log_e_max": state.log_e_max,
        "e_max": sci_decimal(state.log_e_max),
        "first_crossing_t": state.first_crossing_t,
        "alarmed": state.alarmed,
    }


class Monitor:
    """单一计划下的全部流。只读取已确认0/1结果，不控制现场设备。"""

    def __init__(self, plan):
        self.plan = plan
        self.states = {sid: StreamState(spec) for sid, spec in plan.streams.items()}
        self.accepted = 0

    def inspect(self, event):
        """校验并判重：返回('accept'|'duplicate', 规范化事件)；冲突抛EventConflict。"""
        ev = validate_event(self.plan, event)
        st = self.states[ev["stream_id"]]
        old = st.seen.get(ev["sequence"])
        if old is not None:
            if old != ev["outcome"]:
                raise EventConflict(f"事件内容冲突: {ev['stream_id']}#{ev['sequence']}")
            return "duplicate", ev
        return "accept", ev

    def apply_accepted(self, ev):
        """应用一个已校验未重复的事件；返回(每步记录, 新报警回执)。只处理连续前缀。"""
        st = self.states[ev["stream_id"]]
        st.seen[ev["sequence"]] = ev["outcome"]
        st.buffer[ev["sequence"]] = ev["outcome"]
        self.accepted += 1
        steps, alarms = [], []
        while st.next_seq in st.buffer:
            x = st.buffer.pop(st.next_seq)
            t = st.next_seq
            st.next_seq += 1
            st.n += 1
            st.k += x
            le = log_e(st.rt, st.n, st.k)
            if le > st.log_e_max:
                st.log_e_max = le
            if st.first_crossing_t is None and crosses(st.spec, st.rt, st.n, st.k):
                st.first_crossing_t = t
                alarms.append({
                    "type": "alarm",
                    "plan_id": self.plan.plan_id,
                    "stream_id": ev["stream_id"],
                    "t": t,
                    "log_e": le,
                    "e": sci_decimal(le),
                    "alpha": str(st.spec.alpha),
                    "threshold": str(Fraction(1, 1) / st.spec.alpha),
                })
            steps.append({
                "stream_id": ev["stream_id"],
                "t": t,
                "sequence": t,
                "outcome": x,
                "n": st.n,
                "k": st.k,
                "proportion": f"{st.k}/{st.n}",
                "proportion_value": st.k / st.n,
                "log_e": le,
                "e": sci_decimal(le),
                "log_e_max": st.log_e_max,
                "e_max": sci_decimal(st.log_e_max),
                "first_crossing_t": st.first_crossing_t,
                "alarmed": st.alarmed,
                "alpha": str(st.spec.alpha),
                "threshold": str(Fraction(1, 1) / st.spec.alpha),
            })
        return steps, alarms

    def ingest(self, event):
        """受理一个事件：同内容重传幂等，冲突拒绝。"""
        kind, ev = self.inspect(event)
        if kind == "duplicate":
            return {"status": "duplicate", "stream_id": ev["stream_id"], "sequence": ev["sequence"], "steps": [], "alarms": []}
        steps, alarms = self.apply_accepted(ev)
        return {"status": "applied", "stream_id": ev["stream_id"], "sequence": ev["sequence"], "steps": steps, "alarms": alarms}

    def status(self):
        """当前快照：假设、误报预算与每流证据；e值与当前比例分列。"""
        return {
            "plan_id": self.plan.plan_id,
            "global_alpha": str(self.plan.global_alpha),
            "sum_alpha": str(sum((s.alpha for s in self.plan.streams.values()), Fraction(0))),
            "assumptions": ASSUMPTIONS,
            "events_accepted": self.accepted,
            "streams": {sid: _stream_view(st) for sid, st in sorted(self.states.items())},
        }
