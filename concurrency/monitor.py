#!/usr/bin/env python3
"""Sample Trino cluster state every N seconds while load runs.

Writes concurrency/results/monitor.csv:
  ts, running, queued, finished, failed, nodes_cpu?, nodes_mem?
Rows are the truth — they come from system.runtime.queries, not the client.

Usage: ./concurrency/monitor.py [interval_s] [duration_s]
"""
import csv
import os
import env  # noqa: F401  (loads credentials like scripts/lib.sh)
import sys
import time

import trino.auth
import trino.dbapi
import urllib3

SOURCE = "aidp-concurrency"


def main():
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 0  # 0 = forever
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    pw = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    urllib3.disable_warnings()
    conn = trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
        http_scheme="https", user=user,
        auth=trino.auth.BasicAuthentication(user, pw),
        verify=False, request_timeout=60, source="aidp-concurrency-monitor")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "results", "monitor.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    new = not os.path.exists(out)
    fh = open(out, "a", newline="", buffering=1)
    w = csv.writer(fh)
    if new:
        w.writerow(["ts", "running", "queued", "planning", "waiting_for_resources",
                    "failed", "nodes", "cluster_queries"])

    t0 = time.time()
    cur = conn.cursor()
    while True:
        try:
            cur.execute(
                "SELECT state, count(*) FROM system.runtime.queries "
                f"WHERE source='{SOURCE}' GROUP BY state")
            states = {r[0].upper(): r[1] for r in cur.fetchall()}
            cur.execute("SELECT count(*) FROM system.runtime.queries WHERE state='RUNNING'")
            cluster_running = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM system.runtime.nodes WHERE state='active'")
            nodes = cur.fetchone()[0]
        except Exception as e:  # noqa: BLE001
            states, cluster_running, nodes = {}, -1, -1
            print(f"monitor query error: {e}")
        ts = time.strftime("%H:%M:%S")
        w.writerow([ts, states.get("RUNNING", 0), states.get("QUEUED", 0),
                    states.get("PLANNING", 0), states.get("WAITING_FOR_RESOURCES", 0),
                    states.get("FAILED", 0), nodes, cluster_running])
        if duration and time.time() - t0 > duration:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
