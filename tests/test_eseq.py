"""核对序贯e值数学、监测器语义、计划管理与枚举校验。"""
import math
import unittest
from fractions import Fraction
from pathlib import Path

from analysis_data.io import load
from eseq.enumcheck import check_plan, exceedance_probability
from eseq.evalue import StreamRuntime, crosses, exact_e, log_e, sci_decimal
from eseq.monitor import EventConflict, Monitor
from eseq.plan import build_plan, canonical_id
from eseq.recompute import stream_prefixes

DATA = Path(__file__).resolve().parent.parent / "data"


def plan_of(name):
    return build_plan(load(DATA / f"{name}.json")[0])


class EValueTests(unittest.TestCase):
    def test_empty_prefix(self):
        spec = plan_of("normal").streams["site-a"]
        self.assertEqual(log_e(StreamRuntime(spec), 0, 0), 0.0)
        self.assertEqual(exact_e(spec, 0, 0), 1)

    def test_mixture_is_product_then_mix(self):
        spec = plan_of("normal").streams["site-a"]  # p0=1/5, q=[2/5,4/5], w=[1/2,1/2]
        # 序列1,0：先在每个j内部连乘再混合
        want = Fraction(1, 2) * 2 * Fraction(3, 4) + Fraction(1, 2) * 4 * Fraction(1, 4)
        self.assertEqual(exact_e(spec, 2, 1), want)
        # 不是每步混合因子的连乘
        per_step = (Fraction(1, 2) * 2 + Fraction(1, 2) * 4) * (Fraction(1, 2) * Fraction(3, 4) + Fraction(1, 2) * Fraction(1, 4))
        self.assertNotEqual(exact_e(spec, 2, 1), per_step)
        self.assertAlmostEqual(log_e(StreamRuntime(spec), 2, 1), math.log(float(want)), places=12)

    def test_hand_computed_prefixes(self):
        spec = plan_of("normal").streams["site-a"]
        recs = stream_prefixes(spec, [1, 1, 0, 1, 0, 0])
        got = [exact_e(spec, r["n"], r["k"]) for r in recs]
        self.assertEqual(got, [1, 3, 10, Fraction(7, 2), 11, Fraction(17, 4), Fraction(35, 16)])

    def test_supermartingale_one_step(self):
        # 任意条件异常概率p<=p0下，单步期望不超过1（精确分数）
        for spec in plan_of("normal").streams.values():
            e1, e0 = exact_e(spec, 1, 1), exact_e(spec, 1, 0)
            for i in range(21):
                p = spec.p0 * i / 20
                self.assertLessEqual(p * e1 + (1 - p) * e0, 1)

    def test_exact_boundary(self):
        spec = plan_of("exact-threshold").streams["one"]
        rt = StreamRuntime(spec)
        self.assertEqual(exact_e(spec, 2, 2), 4)
        self.assertFalse(crosses(spec, rt, 1, 1))
        self.assertTrue(crosses(spec, rt, 2, 2))  # 恰等计入越界
        # 混合情形下的恰等：e = 1/2*(4/3) + 1/2*(3/2) = 17/12 = 1/alpha
        cfg = {"global_alpha": "1",
               "streams": {"m": {"p0": "1/2", "q": ["2/3", "3/4"], "mixture": ["1/2", "1/2"], "alpha": "12/17"}}}
        spec2 = build_plan(cfg).streams["m"]
        rt2 = StreamRuntime(spec2)
        self.assertEqual(exact_e(spec2, 1, 1), Fraction(17, 12))
        self.assertTrue(crosses(spec2, rt2, 1, 1))
        self.assertFalse(crosses(spec2, rt2, 1, 0))

    def test_sci_decimal(self):
        self.assertEqual(sci_decimal(0.0), "1")
        self.assertEqual(sci_decimal(math.log(3.5)), "3.5")
        self.assertTrue(sci_decimal(3000 * math.log(2)).endswith("e+903"))
        self.assertTrue(sci_decimal(3000 * math.log(2 / 3)).endswith("e-529"))


class MonitorTests(unittest.TestCase):
    def ingest_all(self, mon, events):
        out = []
        for ev in events:
            out.append(mon.ingest(ev))
        return out

    def test_normal_no_alarm(self):
        value = load(DATA / "normal.json")[0]
        mon = Monitor(build_plan(value))
        alarms = []
        for res in self.ingest_all(mon, value["events"]):
            alarms += res["alarms"]
        self.assertEqual(alarms, [])
        st = mon.status()["streams"]["site-a"]
        self.assertEqual((st["applied"], st["anomalies"]), (6, 3))
        self.assertEqual(st["proportion"], "3/6")
        self.assertAlmostEqual(st["log_e"], math.log(35 / 16), places=12)
        self.assertAlmostEqual(st["log_e_max"], math.log(11), places=12)

    def test_exact_threshold_alarm_then_continue(self):
        value = load(DATA / "exact-threshold.json")[0]
        mon = Monitor(build_plan(value))
        r1 = mon.ingest(value["events"][0])
        r2 = mon.ingest(value["events"][1])
        self.assertEqual(r1["alarms"], [])
        self.assertEqual(len(r2["alarms"]), 1)
        alarm = r2["alarms"][0]
        self.assertEqual((alarm["t"], alarm["e"], alarm["threshold"]), (2, "4", "4"))
        # 报警后继续记录：不重置、不另领预算、不重复报警
        r3 = mon.ingest({"stream_id": "one", "sequence": 3, "outcome": 0})
        self.assertEqual(r3["alarms"], [])
        st = mon.status()["streams"]["one"]
        self.assertEqual((st["applied"], st["first_crossing_t"]), (3, 2))

    def test_duplicate_idempotent(self):
        value = load(DATA / "normal.json")[0]
        mon = Monitor(build_plan(value))
        mon.ingest(value["events"][0])
        before = mon.status()
        dup = dict(value["events"][0])
        dup["event_id"] = "another-id"  # 同内容重传即使event_id不同也不推进第二次
        self.assertEqual(mon.ingest(dup)["status"], "duplicate")
        self.assertEqual(mon.status(), before)

    def test_conflict_rejected(self):
        value = load(DATA / "invalid-conflicting-event.json")[0]
        mon = Monitor(build_plan(value))
        with self.assertRaises(EventConflict):
            self.ingest_all(mon, value["events"])
        self.assertEqual(mon.status()["streams"]["site-a"]["applied"], 6)

    def test_gap_buffering_matches_in_order(self):
        value = load(DATA / "out-of-order.json")[0]
        mon = Monitor(build_plan(value))
        res = mon.ingest(value["events"][0])  # site-a#3先到：暂存，不计入
        self.assertEqual(res["steps"], [])
        st = mon.status()["streams"]["site-a"]
        self.assertEqual((st["applied"], st["buffered"]), (0, 1))
        self.ingest_all(mon, value["events"][1:])
        ref = Monitor(plan_of("normal"))
        self.ingest_all(ref, load(DATA / "normal.json")[0]["events"])
        self.assertEqual(mon.status()["streams"], ref.status()["streams"])

    def test_step_records(self):
        value = load(DATA / "normal.json")[0]
        mon = Monitor(build_plan(value))
        steps = []
        for res in self.ingest_all(mon, value["events"]):
            steps += res["steps"]
        self.assertEqual(len(steps), 10)
        first = steps[0]
        self.assertEqual((first["stream_id"], first["t"], first["e"], first["e_max"]), ("site-a", 1, "3", "3"))
        self.assertIsNone(first["first_crossing_t"])
        self.assertEqual(steps[-1]["stream_id"], "site-b")


class PlanTests(unittest.TestCase):
    def test_invalid_configs(self):
        bad = [
            {"global_alpha": "1/10", "streams": {"s": {"p0": "1/2", "q": ["1/4"], "mixture": ["1"], "alpha": "1/20"}}},
            {"global_alpha": "1/10", "streams": {"s": {"p0": "1/5", "q": ["2/5"], "mixture": ["1/2"], "alpha": "1/20"}}},
            {"global_alpha": "1/30", "streams": {"s": {"p0": "1/5", "q": ["2/5"], "mixture": ["1"], "alpha": "1/20"}}},
            {"global_alpha": "1/10", "streams": {"s": {"p0": "1/5", "q": ["2/5"], "mixture": ["1"], "alpha": "1/20", "x": 1}}},
            {"global_alpha": "1/10", "streams": {"s": {"p0": "0.2", "q": ["2/5"], "mixture": ["1"], "alpha": "1/20"}}},
        ]
        for cfg in bad:
            with self.subTest(cfg=cfg):
                with self.assertRaises(ValueError):
                    build_plan(cfg)

    def test_plan_identity(self):
        a = canonical_id(load(DATA / "normal.json")[0])
        b = canonical_id(load(DATA / "out-of-order.json")[0])
        self.assertEqual(a, b)  # 事件不属于计划身份
        changed = {"global_alpha": "1/10",
                   "streams": {"site-a": {"p0": "1/5", "q": ["2/5", "4/5"], "mixture": ["1/2", "1/2"], "alpha": "1/20"},
                               "site-b": {"p0": "1/4", "q": ["1/2"], "mixture": ["1"], "alpha": "1/30"}}}
        self.assertNotEqual(a, canonical_id(changed))  # 改alpha即另一个计划


class EnumTests(unittest.TestCase):
    def test_within_budget(self):
        plan = plan_of("normal")
        out = check_plan(plan, 10)
        for s in out["streams"]:
            self.assertTrue(s["within_budget"], s)
        self.assertTrue(out["joint_bound_ok"])

    def test_exact_threshold_stream(self):
        plan = plan_of("exact-threshold")
        pr = exceedance_probability(plan.streams["one"], 8)
        self.assertGreater(pr, 0)  # 两个成功即越界，确有越界路径
        self.assertLessEqual(pr, Fraction(1, 4))

    def test_lower_p_also_within(self):
        spec = plan_of("normal").streams["site-b"]
        pr = exceedance_probability(spec, 10, p=Fraction(1, 10))
        self.assertLessEqual(pr, spec.alpha)


class DemoTests(unittest.TestCase):
    def test_underflow(self):
        from eseq.demo import demo_underflow
        d = demo_underflow()
        self.assertEqual(d["naive_float64_all_fail"], 0.0)
        self.assertEqual(d["naive_float64_all_success"], "inf")
        self.assertTrue(math.isfinite(d["log_e_all_fail"]))
        self.assertLess(d["crosscheck_rel_err"], 1e-12)
        self.assertTrue(d["exact_fraction_check"])

    def test_scenarios(self):
        from eseq.demo import demo_exact_threshold, demo_gap_fill, demo_restart
        self.assertTrue(demo_exact_threshold()["alarm_fired_at_t2"])
        g = demo_gap_fill()
        self.assertEqual(g["applied_while_gap"], 0)
        self.assertEqual(g["drained_on_fill"], [2, 3])
        self.assertTrue(g["matches_in_order"])
        r = demo_restart()
        self.assertTrue(r["reopened_equal"] and r["continued_equal"])


if __name__ == "__main__":
    unittest.main()
