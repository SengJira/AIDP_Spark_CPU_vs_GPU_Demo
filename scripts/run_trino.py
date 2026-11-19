#!/usr/bin/env python3
"""Run sql/starburst_compare.sql (or any .sql file) against DDAE via Basic auth.

Usage: ./scripts/run_trino.py [sql-file]
Env:   DDAE_USER, DDAE_PASSWORD (falls back to TRINO_USER/TRINO_PASS),
       DDAE_HOST (default ddae.lab9bgp.com), TRINO_VERIFY (default false)
"""
import os
import sys

import trino.auth
import trino.dbapi
import urllib3

SQL_FILE = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "sql", "starburst_compare.sql")


def main():
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    password = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    if not user or not password:
        sys.exit("set DDAE_USER/DDAE_PASSWORD (or TRINO_USER/TRINO_PASS)")
    if os.environ.get("TRINO_VERIFY", "false").lower() != "true":
        urllib3.disable_warnings()
        verify = False
    else:
        verify = True

    conn = trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"),
        port=443,
        http_scheme="https",
        user=user,
        auth=trino.auth.BasicAuthentication(user, password),
        verify=verify,
        request_timeout=300,
    )
    cur = conn.cursor()

    with open(SQL_FILE) as fh:
        text = fh.read()

    # strip '--' comments line-by-line BEFORE splitting on ';'
    cleaned = "\n".join(
        ln[:ln.find("--")] if "--" in ln else ln for ln in text.splitlines()
    )
    stmts = [s.strip() for s in cleaned.split(";") if s.strip()]

    for i, stmt in enumerate(stmts, 1):
        head = " ".join(stmt.split())[:90]
        print(f"\n=== [{i}/{len(stmts)}] {head} ...")
        try:
            cur.execute(stmt)
        except Exception as e:  # noqa: BLE001
            print(f"    ERROR: {e}")
            continue
        try:
            rows = cur.fetchall()
        except Exception:
            rows = None
        if rows:
            cols = [d[0] for d in cur.description]
            print("    " + " | ".join(cols))
            for r in rows[:30]:
                print("    " + " | ".join("null" if v is None else str(v) for v in r))
            if len(rows) > 30:
                print(f"    ... ({len(rows)} rows total)")


if __name__ == "__main__":
    main()
