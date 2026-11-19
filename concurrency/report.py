#!/usr/bin/env python3
"""Turn ramp.log + monitor.csv + query_log.csv into:
  - results/report.md        (per-step + per-persona tables, headline line)
  - results/running_queries.png, qps_over_time.png, p95_by_persona.png
  - rows in js_manuf_ice.aidp_demo.concurrency_results (one per step)

Step boundaries are parsed from ramp.log timestamps; monitor.csv and
query_log.csv carry HH:MM:SS clocks so they segment cleanly.

Usage: ./concurrency/report.py
"""
import csv
import datetime
import os
import statistics
import sys
from collections import defaultdict

import trino.auth
import trino.dbapi
import urllib3

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
FMT = "%H:%M:%S"


def _t(s):
    return datetime.datetime.strptime(s, FMT)


def steps():
    out = []
    path = os.path.join(RES, "ramp.log")
    if not os.path.exists(path):
        return out
    for line in open(path):
        if "== STEP" not in line:
            continue
        # "== STEP step120: users=120 hold=180s (06:09:08)"
        tag = line.split("STEP")[1].split(":")[0].strip()
        users = int(line.split("users=")[1].split()[0])
        hold = int(line.split("hold=")[1].split("s")[0])
        t0 = _t(line.strip().rstrip(")").split("(")[-1])
        out.append({"tag": tag, "users": users, "hold": hold,
                    "t0": t0, "t1": t0 + datetime.timedelta(seconds=hold)})
    return out


def monitor_rows():
    path = os.path.join(RES, "monitor.csv")
    rows = []
    if os.path.exists(path):
        for r in csv.DictReader(open(path)):
            try:
                r["t"] = _t(r["ts"])
                rows.append(r)
            except ValueError:
                pass
    return rows


def query_rows():
    path = os.path.join(RES, "query_log.csv")
    rows = []
    if os.path.exists(path):
        for r in csv.DictReader(open(path)):
            try:
                rows.append({"persona": r["persona"], "ms": float(r["ms"]),
                             "ok": r["ok"] == "1", "t": _t(r["clock"])})
            except (ValueError, KeyError):
                pass
    return rows


def pct(vals, p):
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = (len(vals) - 1) * p
    f = int(k)
    return vals[f] + (vals[min(f + 1, len(vals) - 1)] - vals[f]) * (k - f)


def in_win(r, s, key="t"):
    return s["t0"] <= r[key] < s["t1"]


def main():
    st = steps()
    mon = monitor_rows()
    qs = query_rows()
    if not st or not qs:
        sys.exit("need ramp.log + query_log.csv — run run_ramp.sh first")

    print(f"steps: {[(s['tag'], s['users']) for s in st]}")
    lines = ["# Concurrency test — Starburst/DDAE", ""]
    table = []
    for s in st:
        mwin = [r for r in mon if in_win(r, s) and int(r["running"]) >= 0]
        qwin = [r for r in qs if in_win(r, s)]
        runs = [int(r["running"]) for r in mwin]
        ques = [int(r["queued"]) for r in mwin]
        n, errs = len(qwin), sum(1 for r in qwin if not r["ok"])
        qps = n / s["hold"]
        by_p = defaultdict(list)
        for r in qwin:
            if r["ok"]:
                by_p[r["persona"]].append(r["ms"])
        table.append({"tag": s["tag"], "users": s["users"],
                      "peak_run": max(runs) if runs else 0,
                      "avg_run": statistics.mean(runs) if runs else 0,
                      "peak_q": max(ques) if ques else 0,
                      "n": n, "errs": errs, "qps": qps,
                      "persona": {p: (pct(v, .5), pct(v, .95), pct(v, .99))
                                  for p, v in by_p.items()}})

    lines += ["| step | users | peak RUNNING | avg RUNNING | peak QUEUED | "
              "queries | errors | err % | QPS |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for t in table:
        er = 100 * t["errs"] / max(t["n"], 1)
        lines.append(f"| {t['tag']} | {t['users']} | {t['peak_run']} | "
                     f"{t['avg_run']:.0f} | {t['peak_q']} | {t['n']} | "
                     f"{t['errs']} | {er:.1f}% | {t['qps']:.1f} |")

    personas = sorted({p for t in table for p in t["persona"]})
    lines += ["", "### p95 latency (ms) per persona per step", "",
              "| step | users | " + " | ".join(personas) + " |",
              "|---|---:|" + "---:|" * len(personas)]
    for t in table:
        lines.append(f"| {t['tag']} | {t['users']} | " +
                     " | ".join(f"{t['persona'].get(p, (0,0,0))[1]:.0f}"
                               for p in personas) + " |")

    s360 = next((t for t in table if t["users"] == 360), None)
    if s360:
        pl = s360["persona"].get("point_lookup", (0, 0, 0))
        er = 100 * s360["errs"] / max(s360["n"], 1)
        lines += ["",
                  f"**360 concurrent users -> {s360['avg_run']:.0f} queries "
                  f"running at once (peak {s360['peak_run']}), "
                  f"{s360['qps']:.0f} QPS sustained, "
                  f"p95 {pl[1]:.0f} ms for point lookups, "
                  f"{s360['errs']} errors ({er:.1f}%)**"]

    md = "\n".join(lines) + "\n"
    with open(os.path.join(RES, "report.md"), "w") as fh:
        fh.write(md)
    print(md)

    # ---------------- charts ----------------
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        x = list(range(len(mon)))
        fig, ax = plt.subplots(figsize=(11, 4))
        ax.plot(x, [int(r["running"]) for r in mon], label="RUNNING")
        ax.plot(x, [int(r["queued"]) for r in mon], label="QUEUED")
        for s in st:
            i = next((i for i, r in enumerate(mon) if r["t"] >= s["t0"]), None)
            if i is not None:
                ax.axvline(i, color="gray", ls=":", lw=0.7)
                ax.text(i, ax.get_ylim()[1] * .95, str(s["users"]),
                        fontsize=8, ha="center")
        ax.set(xlabel="time (5s samples, step labels = users)",
               ylabel="queries",
               title="Trino system.runtime.queries (source=aidp-concurrency)")
        ax.legend(); fig.tight_layout()
        fig.savefig(os.path.join(RES, "running_queries.png"), dpi=110)

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar([t["tag"] for t in table], [t["qps"] for t in table])
        ax.set(ylabel="QPS", title="Queries/sec per step")
        plt.xticks(rotation=30); fig.tight_layout()
        fig.savefig(os.path.join(RES, "qps_over_time.png"), dpi=110)

        fig, ax = plt.subplots(figsize=(10, 4))
        for p in personas:
            ax.plot([t["tag"] for t in table],
                    [t["persona"].get(p, (0, 0, 0))[1] for t in table],
                    marker="o", label=p)
        ax.set(ylabel="p95 ms", title="p95 latency by persona per step")
        ax.legend(fontsize=8); plt.xticks(rotation=30); fig.tight_layout()
        fig.savefig(os.path.join(RES, "p95_by_persona.png"), dpi=110)
        print("charts: running_queries.png, qps_over_time.png, p95_by_persona.png")
    except Exception as e:  # noqa: BLE001
        print(f"charts skipped: {e}")

    # ---------------- starburst ----------------
    try:
        user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
        pw = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
        urllib3.disable_warnings()
        conn = trino.dbapi.connect(
            host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
            http_scheme="https", user=user,
            auth=trino.auth.BasicAuthentication(user, pw),
            verify=False, request_timeout=120)
        cur = conn.cursor()
        cur.execute("SET ROLE sysadmin")
        cur.execute("""CREATE TABLE IF NOT EXISTS
            js_manuf_ice.aidp_demo.concurrency_results
            (run_ts timestamp(6), step varchar, users bigint,
             peak_running bigint, avg_running double, peak_queued bigint,
             queries bigint, errors bigint, error_pct double, qps double)""")
        vals = ",".join(
            f"(current_timestamp,'{t['tag']}',{t['users']},{t['peak_run']},"
            f"{t['avg_run']:.1f},{t['peak_q']},{t['n']},{t['errs']},"
            f"{100*t['errs']/max(t['n'],1):.2f},{t['qps']:.2f})"
            for t in table)
        cur.execute("INSERT INTO js_manuf_ice.aidp_demo.concurrency_results "
                    "VALUES " + vals)
        print("per-step rows -> js_manuf_ice.aidp_demo.concurrency_results")
    except Exception as e:  # noqa: BLE001
        print(f"trino insert skipped: {e}")


if __name__ == "__main__":
    main()
