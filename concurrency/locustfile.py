"""Locust load generator — one Trino connection per virtual user.

Run:
  locust -f concurrency/locustfile.py --host=dummy           # web UI :8089
  locust -f concurrency/locustfile.py --host=dummy --headless \
         -u 60 -r 60 -t 3m --csv results/locust             # headless + CSV

Every query gets random parameters (sampled real keys + domains from
concurrency/results/*.csv) so consecutive queries never hit an identical
cached result. Per-query log lines go to results/query_log.csv.
"""
import csv
import os
import random
import time

import trino.auth
import trino.dbapi
import urllib3
from locust import User, between, events, task

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
SOURCE = "aidp-concurrency"

THINK_TIME = float(os.environ.get("THINK_TIME", "0"))

_pairs, _provinces, _segments, _tiers = [], [], [], []


def _load_domains():
    global _pairs, _provinces, _segments, _tiers
    if _pairs:
        return
    with open(os.path.join(RESULTS, "sampled_keys.csv")) as fh:
        _pairs = [(r[0], int(r[1])) for r in csv.reader(fh) if r and r[0] != "province"]
    with open(os.path.join(RESULTS, "domains.csv")) as fh:
        for r in csv.reader(fh):
            if not r or r[0] == "kind":
                continue
            {"province": _provinces, "segment": _segments,
             "risk_tier": _tiers}.get(r[0], []).append(r[1])


def _month():
    # txn_ts spans 2023-10-26 .. 2024-10-25 in the generated data
    return random.choice([(2023, 11), (2023, 12)] +
                         [(2024, m) for m in range(1, 10)])


def _quarter():
    return random.choice([("2023-11-01", "2024-02-01"),
                          ("2024-02-01", "2024-05-01"),
                          ("2024-05-01", "2024-08-01"),
                          ("2024-08-01", "2024-10-25")])


class TrinoClient:
    """Minimal trino wrapper that fires locust request events + logs."""

    def __init__(self, user, password, host, log_fh):
        urllib3.disable_warnings()
        self._conn = trino.dbapi.connect(
            host=host, port=443, http_scheme="https", user=user,
            auth=trino.auth.BasicAuthentication(user, password),
            verify=False, source=SOURCE, request_timeout=600,
            max_attempts=1)
        self._log = log_fh

    def query(self, persona, sql):
        start = time.perf_counter()
        qid, nrows, err = "", 0, None
        try:
            cur = self._conn.cursor()
            cur.execute(sql)
            qid = getattr(cur, "stats", {}).get("queryId", "") or ""
            rows = cur.fetchall()
            nrows = len(rows)
        except Exception as e:  # noqa: BLE001
            err = e
        ms = (time.perf_counter() - start) * 1000
        ok = err is None
        events.request.fire(
            request_type="trino", name=persona,
            response_time=ms, response_length=nrows,
            exception=err)
        self._log.writerow([persona, f"{ms:.1f}", nrows, int(ok),
                            qid, time.strftime("%H:%M:%S"),
                            "" if ok else str(err)[:200].replace("\n", " ")])
        self._log.flush if False else None


class TrinoUser(User):
    wait_time = between(THINK_TIME, THINK_TIME)
    abstract = True

    def on_start(self):
        _load_domains()
        if not hasattr(self.environment, "_qlog"):
            self.environment._qlog = open(
                os.path.join(RESULTS, "query_log.csv"), "a", newline="", buffering=1)
            w = csv.writer(self.environment._qlog)
            if self.environment._qlog.tell() == 0:
                w.writerow(["persona", "ms", "rows", "ok", "query_id", "clock", "error"])
        self._logw = csv.writer(self.environment._qlog)
        user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
        pw = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
        host = os.environ.get("DDAE_HOST", "ddae.lab9bgp.com")
        self.client = TrinoClient(user, pw, host, self._logw)

    # ---------- personas (weights via @task) ----------

    @task(40)
    def fraud_app_lookup(self):
        prov, cid = random.choice(_pairs)
        self.client.query(
            "point_lookup",
            f"SELECT * FROM js_manuf_ice.aidp_demo.customer_features_serving "
            f"WHERE province='{prov}' AND customer_id={cid}")

    @task(25)
    def branch_dashboard(self):
        prov = random.choice(_provinces)
        self.client.query(
            "province_tile",
            f"SELECT segment, count(*), sum(flagged_count), round(avg(total_amount),2) "
            f"FROM js_manuf_ice.aidp_demo.customer_features_serving "
            f"WHERE province='{prov}' GROUP BY segment")

    @task(15)
    def risk_analyst(self):
        prov, tier = random.choice(_provinces), random.choice(_tiers)
        self.client.query(
            "top_risky",
            f"SELECT customer_id, segment, flagged_count, round(total_amount,2) "
            f"FROM js_manuf_ice.aidp_demo.customer_features_serving "
            f"WHERE province='{prov}' AND risk_tier='{tier}' "
            f"ORDER BY flagged_count DESC, total_amount DESC LIMIT 20")

    @task(10)
    def exec_dashboard(self):
        self.client.query(
            "exec_full",
            "SELECT province, segment, risk_tier, count(*), "
            "sum(flagged_count), round(avg(total_amount),2) "
            "FROM js_manuf_ice.aidp_demo.customer_features_serving "
            "GROUP BY province, segment, risk_tier")

    @task(7)
    def merchant_analytics(self):
        y, m = _month()
        self.client.query(
            "merchant_month",
            f"SELECT m.category, count(*), round(sum(t.amount),2) "
            f"FROM js_manuf_ice.aidp_demo.transactions t "
            f"JOIN js_manuf_ice.aidp_demo.merchants m ON t.merchant_id=m.merchant_id "
            f"WHERE year(t.txn_ts)={y} AND month(t.txn_ts)={m} "
            f"GROUP BY m.category")

    @task(3)
    def heavy_adhoc(self):
        a, b = _quarter()
        self.client.query(
            "heavy_quarter",
            f"SELECT c.province, m.category, count(*), round(sum(t.amount),2) "
            f"FROM js_manuf_ice.aidp_demo.transactions t "
            f"JOIN js_manuf_ice.aidp_demo.customers c ON t.customer_id=c.customer_id "
            f"JOIN js_manuf_ice.aidp_demo.merchants m ON t.merchant_id=m.merchant_id "
            f"WHERE t.txn_ts >= timestamp '{a}' AND t.txn_ts < timestamp '{b}' "
            f"GROUP BY c.province, m.category")


class ConcurrencyUser(TrinoUser):
    pass
