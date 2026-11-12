"""核对持久化、重放一致、独立审计与命令行。"""
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from analysis_data.io import load
from eseq.recompute import audit_events, compare_with_status
from eseq.store import CorruptionError, Store

DATA = Path(__file__).resolve().parent.parent / "data"


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)
        self.value = load(DATA / "normal.json")[0]

    def ingest_all(self, mon, events):
        for ev in events:
            mon.ingest(ev)

    def journal(self, plan_id):
        return self.store.root / plan_id / "journal.jsonl"

    def test_restart_replay_consistent(self):
        plan, is_new = self.store.open_plan(self.value)
        self.assertTrue(is_new)
        mon = self.store.monitor(plan.plan_id)
        self.ingest_all(mon, self.value["events"][:5])
        before = mon.status()
        reopened = self.store.monitor(plan.plan_id)  # 进程退出后重开
        self.assertEqual(reopened.status(), before)
        self.ingest_all(reopened, self.value["events"][5:])
        again = self.store.monitor(plan.plan_id)
        self.assertEqual(again.status(), reopened.status())

    def test_duplicate_not_journaled(self):
        plan, _ = self.store.open_plan(self.value)
        mon = self.store.monitor(plan.plan_id)
        mon.ingest(self.value["events"][0])
        size = self.journal(plan.plan_id).stat().st_size
        self.assertEqual(mon.ingest(dict(self.value["events"][0]))["status"], "duplicate")
        self.assertEqual(self.journal(plan.plan_id).stat().st_size, size)

    def test_conflict_not_journaled(self):
        plan, _ = self.store.open_plan(self.value)
        mon = self.store.monitor(plan.plan_id)
        mon.ingest(self.value["events"][0])
        size = self.journal(plan.plan_id).stat().st_size
        bad = {"stream_id": "site-a", "sequence": 1, "outcome": 0}
        with self.assertRaises(ValueError):
            mon.ingest(bad)
        self.assertEqual(self.journal(plan.plan_id).stat().st_size, size)

    def test_new_config_requires_new_plan(self):
        plan1, new1 = self.store.open_plan(self.value)
        again, new2 = self.store.open_plan(self.value)
        self.assertTrue(new1)
        self.assertFalse(new2)
        self.assertEqual(plan1.plan_id, again.plan_id)
        self.assertEqual(plan1.opened_at, again.opened_at)  # 旧计划保持原样
        changed = json.loads(json.dumps({k: v for k, v in self.value.items() if k != "events"}))
        changed["streams"]["site-b"]["alpha"] = "1/30"
        plan2, new3 = self.store.open_plan(changed)
        self.assertTrue(new3)
        self.assertNotEqual(plan1.plan_id, plan2.plan_id)
        self.assertEqual(len(self.store.list_plans()), 2)  # 旧计划保留

    def test_audit_consistent(self):
        plan, _ = self.store.open_plan(self.value)
        mon = self.store.monitor(plan.plan_id)
        self.ingest_all(mon, self.value["events"])
        audit = audit_events(plan, self.value["events"])
        self.assertEqual(compare_with_status(audit, mon.status()), [])

    def test_alarm_receipt_replay_and_tamper(self):
        value = load(DATA / "exact-threshold.json")[0]
        plan, _ = self.store.open_plan(value)
        mon = self.store.monitor(plan.plan_id)
        self.ingest_all(mon, value["events"])
        text = self.journal(plan.plan_id).read_text("utf8")
        self.assertIn('"type":"alarm"', text)
        reopened = self.store.monitor(plan.plan_id)
        self.assertEqual(reopened.status()["streams"]["one"]["first_crossing_t"], 2)
        lines = text.splitlines()
        rec = json.loads(lines[-1])
        rec["t"] = 99  # 篡改回执
        lines[-1] = json.dumps(rec)
        self.journal(plan.plan_id).write_text("\n".join(lines) + "\n", encoding="utf8")
        with self.assertRaises(CorruptionError):
            self.store.monitor(plan.plan_id)


class CliTests(unittest.TestCase):
    def run_cli(self, argv):
        from eseq.cli import main
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(argv)
        return rc, buf.getvalue()

    def test_run_audit_enumcheck_demo(self):
        with tempfile.TemporaryDirectory() as d:
            rc, out = self.run_cli(["run", str(DATA / "normal.json"), "--root", d])
            self.assertEqual(rc, 0)
            run_out = json.loads(out)
            self.assertEqual(run_out["alarms"], [])
            self.assertEqual(len(run_out["steps"]), 10)
            pid = run_out["plan_id"]
            rc, out = self.run_cli(["audit", d, pid])
            self.assertEqual(rc, 0)
            self.assertTrue(json.loads(out)["consistent"])
            rc, out = self.run_cli(["plans", d])
            self.assertEqual(rc, 0)
            self.assertEqual(len(json.loads(out)["plans"]), 1)
            rc, out = self.run_cli(["status", d, pid])
            self.assertEqual(rc, 0)
            self.assertIn("assumptions", json.loads(out))
        rc, out = self.run_cli(["enumcheck", str(DATA / "exact-threshold.json"), "--n", "8"])
        self.assertEqual(rc, 0)
        self.assertTrue(json.loads(out)["streams"][0]["within_budget"])
        rc, out = self.run_cli(["demo"])
        self.assertEqual(rc, 0)
        self.assertIn("underflow", json.loads(out))

    def test_run_out_of_order_and_conflict(self):
        rc, out = self.run_cli(["run", str(DATA / "out-of-order.json")])
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out)["duplicates"], 1)
        rc, _ = self.run_cli(["run", str(DATA / "invalid-conflicting-event.json")])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
