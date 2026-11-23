"""Env loader for the concurrency scripts — mirrors scripts/lib.sh load_env.

Priority: repo .env -> ~/.cnc-demo-credentials -> existing process env.
Legacy lab names (S3_*, TRINO_*) map to the demo names (OBS_*, DDAE_*).
Import once at the top of each entry point; it mutates os.environ.
"""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)


def _parse(path):
    vals = {}
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return vals


def load():
    src = (os.path.join(_REPO, ".env")
           if os.path.exists(os.path.join(_REPO, ".env"))
           else os.path.expanduser("~/.cnc-demo-credentials"))
    for k, v in _parse(src).items():
        os.environ.setdefault(k, v)

    e = os.environ
    e.setdefault("OBS_ACCESS_KEY", e.get("S3_ACCESS_KEY", ""))
    e.setdefault("OBS_SECRET_KEY", e.get("S3_SECRET_KEY", ""))
    e.setdefault("OBS_ENDPOINT", e.get("S3_ENDPOINT", "http://172.18.11.31:9020"))
    e.setdefault("DDAE_USER", e.get("TRINO_USER", ""))
    e.setdefault("DDAE_PASSWORD", e.get("TRINO_PASS", ""))
    e.setdefault("DEMO_BUCKET", "js-demo")
    e.setdefault("DDAE_HOST", "ddae.lab9bgp.com")


load()
