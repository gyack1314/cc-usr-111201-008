"""命令行：运行资料文件、查看状态、独立审计、枚举校验、场景演示。只读监测，不发真实消息。"""
import argparse
import json
import sys

from analysis_data.io import load

from .demo import run_all
from .enumcheck import check_plan
from .monitor import Monitor
from .plan import build_plan
from .recompute import audit_events, compare_with_status
from .store import Store


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False))


def _run_file(args):
    value, digest = load(args.file)
    if args.root:
        store = Store(args.root)
        plan, is_new = store.open_plan(value)
        mon = store.monitor(plan.plan_id)
    else:
        plan, is_new = build_plan(value), None
        mon = Monitor(plan)
    steps, alarms, duplicates = [], [], 0
    for ev in value.get("events", []):
        res = mon.ingest(ev)
        if res["status"] == "duplicate":
            duplicates += 1
        steps.extend(res["steps"])
        alarms.extend(res["alarms"])
    _print({
        "source_sha256": digest,
        "plan_id": plan.plan_id,
        "plan_opened": is_new,
        "persistent": bool(args.root),
        "duplicates": duplicates,
        "steps": steps,
        "alarms": alarms,
        "status": mon.status(),
    })
    return 0


def _audit(args):
    store = Store(args.root)
    mon = store.monitor(args.plan_id)
    events = []
    for line in (store.root / args.plan_id / "journal.jsonl").read_text("utf8").splitlines():
        rec = json.loads(line)
        if rec.get("type") == "event":
            events.append({k: rec[k] for k in ("stream_id", "sequence", "outcome")})
    audit = audit_events(mon.plan, events)
    mismatches = compare_with_status(audit, mon.status())
    _print({
        "plan_id": args.plan_id,
        "consistent": not mismatches,
        "mismatches": mismatches,
        "streams": {sid: {k: v for k, v in s.items() if k != "prefixes"} for sid, s in audit["streams"].items()},
    })
    return 0 if not mismatches else 1


def _enumcheck(args):
    value, digest = load(args.file)
    out = check_plan(build_plan(value), args.n)
    out["source_sha256"] = digest
    _print(out)
    ok = all(s["within_budget"] for s in out["streams"]) and out["joint_bound_ok"]
    return 0 if ok else 1


def main(argv=None):
    p = argparse.ArgumentParser(prog="eseq", description="序贯e值监测：只读取已确认0/1结果，不控制现场设备，不发送真实消息。")
    sub = p.add_subparsers(dest="command", required=True)
    pr = sub.add_parser("run", help="开启/复用计划并按交付顺序受理资料文件中的事件")
    pr.add_argument("file")
    pr.add_argument("--root", help="持久化根目录；缺省为内存运行")
    ps = sub.add_parser("status", help="查看计划当前状态（假设、预算、证据、当前比例）")
    ps.add_argument("root")
    ps.add_argument("plan_id")
    pl = sub.add_parser("plans", help="列出全部已开启计划（旧计划保留）")
    pl.add_argument("root")
    pa = sub.add_parser("audit", help="由日志中的原始事件独立重算全部前缀并与当前状态比对")
    pa.add_argument("root")
    pa.add_argument("plan_id")
    pe = sub.add_parser("enumcheck", help="小样本枚举Bernoulli(p0)路径，核算越界概率不超过预算")
    pe.add_argument("file")
    pe.add_argument("--n", type=int, default=12)
    sub.add_parser("demo", help="演示长链下溢、恰等阈值、缺口补齐与进程重启")
    args = p.parse_args(argv)
    try:
        if args.command == "run":
            return _run_file(args)
        if args.command == "status":
            _print(Store(args.root).monitor(args.plan_id).status())
            return 0
        if args.command == "plans":
            _print({"plans": Store(args.root).list_plans()})
            return 0
        if args.command == "audit":
            return _audit(args)
        if args.command == "enumcheck":
            return _enumcheck(args)
        if args.command == "demo":
            _print(run_all())
            return 0
    except (ValueError, KeyError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
