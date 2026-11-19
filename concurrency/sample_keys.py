#!/usr/bin/env python3
"""Sample real (province, customer_id) pairs + parameter domains once, so
locust lookups always hit real rows and params stay random per query.

Writes concurrency/results/sampled_keys.csv
"""
import csv
import os
import sys

import trino.auth
import trino.dbapi
import urllib3


def main():
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    pw = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    if not user:
        sys.exit("set DDAE_USER/DDAE_PASSWORD")
    urllib3.disable_warnings()
    conn = trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
        http_scheme="https", user=user,
        auth=trino.auth.BasicAuthentication(user, pw),
        verify=False, request_timeout=300)
    cur = conn.cursor()

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(out_dir, exist_ok=True)

    cur.execute("""SELECT province, customer_id
                   FROM js_manuf_ice.aidp_demo.customer_features_serving
                   TABLESAMPLE BERNOULLI(0.5) LIMIT 10000""")
    pairs = cur.fetchall()
    cur.execute("SELECT DISTINCT province FROM js_manuf_ice.aidp_demo.customer_features_serving")
    provinces = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT segment FROM js_manuf_ice.aidp_demo.customer_features_serving")
    segments = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT risk_tier FROM js_manuf_ice.aidp_demo.customer_features_serving")
    tiers = [r[0] for r in cur.fetchall()]

    path = os.path.join(out_dir, "sampled_keys.csv")
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["province", "customer_id"])
        w.writerows(pairs)
    meta = os.path.join(out_dir, "domains.csv")
    with open(meta, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["kind", "value"])
        for p in provinces:
            w.writerow(["province", p])
        for s in segments:
            w.writerow(["segment", s])
        for t in tiers:
            w.writerow(["risk_tier", t])
    print(f"sampled {len(pairs)} key pairs -> {path}")
    print(f"domains: {len(provinces)} provinces, {len(segments)} segments, {len(tiers)} tiers")


if __name__ == "__main__":
    main()
