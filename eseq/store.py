"""持久化：计划注册表、事件日志与报警回执。日志是唯一事实来源，进程退出后重放一致。"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .monitor import Monitor
from .plan import build_plan, canonical_id, parse_config


class CorruptionError(ValueError):
    """日志与事件流重推导结果不一致。"""


def _dump(obj):
    return json.dumps(obj, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _atomic_write(path, text):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf8")
    os.replace(tmp, path)


def _config_only(config):
    return {k: v for k, v in config.items() if k != "events"}


class Store:
    """一个部署根目录：plans.json注册表 + 每计划一个目录（plan.json + journal.jsonl）。"""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def list_plans(self):
        path = self.root / "plans.json"
        if not path.exists():
            return []
        return json.loads(path.read_text("utf8"))["plans"]

    def _register(self, plan_id, opened_at):
        plans = self.list_plans()
        plans.append({"plan_id": plan_id, "opened_at": opened_at})
        _atomic_write(self.root / "plans.json", _dump({"plans": plans}))

    def open_plan(self, config, opened_at=None):
        """相同配置回到同一计划；任何配置变更自然成为新计划，旧计划保留。返回(plan, is_new)。"""
        pid = canonical_id(config)
        pdir = self.root / pid
        meta_path = pdir / "plan.json"
        if pdir.exists():
            meta = json.loads(meta_path.read_text("utf8"))
            if meta.get("plan_id") != pid or canonical_id(meta.get("config", {})) != pid:
                raise CorruptionError(f"计划目录{pid}内容与身份不符")
            return build_plan(meta["config"], meta.get("opened_at", "")), False
        parse_config(config)  # 先校验后落盘
        opened_at = opened_at or datetime.now(timezone.utc).isoformat()
        pdir.mkdir()
        _atomic_write(meta_path, _dump({"plan_id": pid, "opened_at": opened_at, "config": _config_only(config)}))
        (pdir / "journal.jsonl").touch()
        self._register(pid, opened_at)
        return build_plan(config, opened_at), True

    def get_plan(self, plan_id):
        meta_path = self.root / plan_id / "plan.json"
        if not meta_path.exists():
            raise ValueError(f"未知计划: {plan_id}")
        meta = json.loads(meta_path.read_text("utf8"))
        return build_plan(meta["config"], meta.get("opened_at", ""))

    def monitor(self, plan_id):
        return PersistentMonitor(self, self.get_plan(plan_id))


class PersistentMonitor:
    """把Monitor的状态变化追加到日志；打开时重放事件并校验报警回执。"""

    def __init__(self, store, plan):
        self.store = store
        self.plan = plan
        self.monitor = Monitor(plan)
        self.journal_path = store.root / plan.plan_id / "journal.jsonl"
        self._replay()

    def _append(self, record):
        with self.journal_path.open("a", encoding="utf8") as fh:
            fh.write(_dump(record) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _replay(self):
        derived, recorded = [], []
        for line in self.journal_path.read_text("utf8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            kind = rec.get("type")
            if kind == "event":
                verdict, ev = self.monitor.inspect({k: v for k, v in rec.items() if k != "type"})
                if verdict != "accept":
                    raise CorruptionError("日志中出现未受理或重复事件")
                _, alarms = self.monitor.apply_accepted(ev)
                derived.extend(alarms)
            elif kind == "alarm":
                recorded.append(rec)
            else:
                raise CorruptionError(f"日志含未知记录类型: {kind!r}")
        if recorded != derived[: len(recorded)]:
            raise CorruptionError("报警回执与事件流重推导不一致")
        for alarm in derived[len(recorded):]:  # 修复崩溃未来得及写回执的情形
            self._append(alarm)

    def ingest(self, event):
        """先写日志再更新状态；同内容重传不写第二次。"""
        verdict, ev = self.monitor.inspect(event)
        if verdict == "duplicate":
            return {"status": "duplicate", "stream_id": ev["stream_id"], "sequence": ev["sequence"], "steps": [], "alarms": []}
        self._append({"type": "event", **ev})
        steps, alarms = self.monitor.apply_accepted(ev)
        for alarm in alarms:
            self._append(alarm)
        return {"status": "applied", "stream_id": ev["stream_id"], "sequence": ev["sequence"], "steps": steps, "alarms": alarms}

    def status(self):
        return self.monitor.status()
