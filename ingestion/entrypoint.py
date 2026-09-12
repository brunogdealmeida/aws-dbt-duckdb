import os
import sys
import subprocess

mode = os.getenv("MODE", "dbt")

if mode == "dbt":
    cmd = ["dbt", "run"]
elif mode == "dbt-test":
    cmd = ["dbt", "test"]
elif mode == "dbt-build":
    cmd = ["dbt", "build"]
elif mode == "ingest":
    cmd = [sys.executable, "-m", "ingestion.ingest_csv"]
else:
    raise SystemExit(f"Unsupported MODE={mode}")

subprocess.run(cmd, check=True)
