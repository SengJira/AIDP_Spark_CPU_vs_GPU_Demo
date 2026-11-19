#!/usr/bin/env python3
"""Report anything that caps query concurrency BEFORE running load.

Prints (and writes concurrency/results/check_limits.txt):
  - cluster nodes from system.runtime.nodes
  - resource-group limits if readable (hardConcurrencyLimit, maxQueued, mem)
  - the demo user's identity / session capabilities
  - whether the Iceberg catalog has result/Warp-Speed-style caching enabled
Honesty rule: if the resource group caps running queries < 360, we report
the real number and where it is configured — we never call queued=running.
"""
import os
import sys
import time

import trino.auth
import trino.dbapi
import urllib3


def conn():
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    pw = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    if not user:
        sys.exit("set DDAE_USER/DDAE_PASSWORD")
    urllib3.disable_warnings()
    return trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
        http_scheme="https", user=user,
        auth=trino.auth.BasicAuthentication(user, pw),
        verify=False, request_timeout=120)


def main():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(out_dir, exist_ok=True)
    lines = []

    def emit(s=""):
        print(s)
        lines.append(s)

    cur = conn().cursor()
    emit(f"== check_limits {time.strftime('%Y-%m-%d %H:%M:%S')} ==")

    # --- cluster nodes ---
    emit("\n-- system.runtime.nodes --")
    try:
        cur.execute("SELECT node_id, state, coordinator FROM system.runtime.nodes")
        rows = cur.fetchall()
        coord = [r for r in rows if r[2]]
        workers = [r for r in rows if not r[2]]
        emit(f"nodes: {len(rows)} total ({len(coord)} coordinator, {len(workers)} workers)")
        for r in rows:
            emit(f"  {r[0]} state={r[1]} coordinator={r[2]}")
    except Exception as e:
        emit(f"  ERROR: {e}")

    # --- resource groups ---
    emit("\n-- resource groups --")
    rg_found = False
    for q, label in [
        ("SELECT * FROM system.runtime.resource_groups", "system.runtime.resource_groups"),
        ("SELECT * FROM system.resource_groups", "system.resource_groups"),
    ]:
        try:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            rg_found = True
            emit(f"{label}:")
            emit("  " + " | ".join(cols))
            for r in cur.fetchall():
                emit("  " + " | ".join(str(v) for v in r))
        except Exception as e:
            emit(f"  {label}: {str(e)[:120]}")
    if not rg_found:
        emit("  resource-group tables not readable via SQL — check the")
        emit("  Starburst UI > Resource Groups or ask the admin for the")
        emit("  user's hardConcurrencyLimit / maxQueued / mem limits.")

    # --- session limits / current queries caps ---
    emit("\n-- relevant session/catalog properties --")
    for q in [
        "SELECT name, value FROM system.metadata.session_properties WHERE name LIKE '%concurr%' OR name LIKE '%queue%' OR name LIKE '%cache%' OR name LIKE '%warp%' LIMIT 30",
    ]:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            if rows:
                for r in rows:
                    emit(f"  {r[0]} = {r[1]}")
            else:
                emit("  (no matching session properties)")
        except Exception as e:
            emit(f"  {str(e)[:150]}")

    emit("\n-- catalog properties (js_manuf_ice) --")
    try:
        cur.execute("SELECT * FROM system.metadata.catalogs WHERE catalog_name='js_manuf_ice'")
        emit("  " + str(cur.fetchall()))
    except Exception as e:
        emit(f"  {str(e)[:150]}")

    emit("\n-- live query state now (system.runtime.queries) --")
    try:
        cur.execute("SELECT state, count(*) FROM system.runtime.queries GROUP BY state")
        for r in cur.fetchall():
            emit(f"  {r[0]}: {r[1]}")
        cur.execute("SELECT resource_group_id, count(*) "
                    "FROM system.runtime.queries GROUP BY 1")
        for r in cur.fetchall():
            emit(f"  resource_group_id={r[0]}: {r[1]} queries")
    except Exception as e:
        emit(f"  {str(e)[:150]}")
    emit("\n-- observed empirically --")
    emit("  * EXCEEDED_LOCAL_MEMORY_LIMIT on large DDL: per-node query")
    emit("    memory cap ~167 MB -> heavy/ad-hoc queries WILL fail under load;")
    emit("    that is honest data for the report, not a test bug.")

    emit("\nACTION: compare 'hardConcurrencyLimit' (if shown) with 360.")
    emit("If the demo user's resource group caps RUNNING below 360,")
    emit("STOP and report the limit — the test reports queued separately.")

    path = os.path.join(out_dir, "check_limits.txt")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
