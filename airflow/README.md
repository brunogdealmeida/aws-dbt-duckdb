# Airflow DAG for this project

`dags/dbt_lakehouse_dag.py` triggers the **same** ECS Fargate task this
project already uses for EventBridge Scheduler and the manual GitHub
Actions dispatch (`MODE=dbt-build` on the `aws-duckdb-lakehouse-dev` task
definition). Airflow doesn't run dbt itself — it calls `ecs:RunTask` and
waits, reusing the Docker image CI/CD already builds and tests. That keeps
the dbt/DuckDB/Iceberg toolchain and S3 Tables credentials in one place
instead of duplicating them into the Airflow environment too.

There are two ways to run it: against **your own existing Airflow instance**
(§A), or against a **persistent local instance** already set up in this repo
(§B) — a plain `venv` (no Docker), started/stopped with two scripts, useful
when your own instance isn't available or for quick standalone testing.

## A. Using your own existing Airflow instance

### 1. Copy the DAG

Copy (or symlink) `dags/dbt_lakehouse_dag.py` into your Airflow instance's
DAGs folder.

### 2. Install the AWS provider

```bash
pip install apache-airflow-providers-amazon
```

If your Airflow runs via the official docker-compose, add it through
`_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-amazon` (dev only —
for anything long-lived, bake it into a custom image instead).

### 3. Give the Airflow worker AWS credentials

Pick one:

- **Mount `~/.aws` read-only** into the Airflow containers and leave
  `AWS_CONN_ID = None` in the DAG (boto3's default credential chain).
  Simplest if you're reusing the local `terraform-admin` IAM user already
  set up for this project (see `ARCHITECTURE.md` §1.3) — it already has
  every permission needed via `AdministratorAccess`.
- **Airflow Connection**: Admin → Connections → new connection of type
  "Amazon Web Services", then set `AWS_CONN_ID` in the DAG to its
  connection id.

Whichever identity you use needs, at minimum:
- `ecs:RunTask` on `arn:aws:ecs:us-east-1:<account>:task-definition/aws-duckdb-lakehouse-dev:*`
- `iam:PassRole` on the `ecs_execution` and `ecs_task` roles
- `ecs:DescribeTasks` and `logs:GetLogEvents` (so the operator can poll
  completion and stream logs into the Airflow UI)

`infra/scheduler.tf`'s `aws_iam_role_policy.scheduler` has the exact policy
if you'd rather create a dedicated IAM entity instead of reusing
`terraform-admin`.

### 4. Run it

In the Airflow UI: DAGs → `dbt_lakehouse` → trigger. Task logs in Airflow
will include the ECS task's CloudWatch output (via `awslogs_group`/
`awslogs_stream_prefix` on the operator).

## B. Persistent local instance (no Docker, no existing Airflow needed)

A plain-Python Airflow (`apache-airflow==3.0.3` + `apache-airflow-providers-amazon`,
`airflow standalone` — webserver/UI + scheduler + triggerer in one process)
lives in `.venv/` + `home/` in this folder, already set up with the DAG.
Both are gitignored — local only, not deployed anywhere.

```bash
cd airflow
./start.sh   # starts it (if not already running), prints the URL + admin password
./stop.sh    # stops it
```

- **UI**: http://localhost:8080 — user `admin`, password printed by
  `start.sh` (also readable any time from
  `home/simple_auth_manager_passwords.json.generated`, e.g.
  `python3 -c "import json; print(json.load(open('home/simple_auth_manager_passwords.json.generated'))['admin'])"`
  — it's generated once on first-ever start and reused after that, so it
  won't change across restarts).
- **AWS credentials**: uses your local default boto3 chain (`AWS_CONN_ID =
  None` in the DAG) — whatever `aws sts get-caller-identity` resolves to in
  your shell (e.g. the `terraform-admin` IAM user set up for this project)
  is what the DAG runs as.
- The DAG is read directly from `airflow/dags/` (via
  `AIRFLOW__CORE__DAGS_FOLDER`, set by `start.sh`) — no copying needed;
  editing the file and re-triggering picks up changes.
- The DAG starts **paused** by default (standard Airflow behavior for newly
  discovered DAGs) — toggle it on in the UI, or trigger it manually
  regardless of pause state (pausing only blocks the *schedule*, not manual
  triggers).
- Logs: `home/standalone.log` (Airflow's own process log) and, per task run,
  in the UI under DAGs → `dbt_lakehouse` → the run → `dbt_build` → Logs.

## Notes

- **This does not replace** the EventBridge Scheduler (`infra/scheduler.tf`,
  daily 03:00 UTC) — the two can coexist; they both just call `ecs:RunTask`
  on the same task definition. If you want Airflow to own scheduling
  instead, set `schedule=None` in the DAG to a cron expression and disable
  (or remove) `infra/scheduler.tf`'s schedule to avoid double-running.
- The 5M-row `stg_orders` model alone takes ~9 minutes to write on
  DuckDB 1.5.5 (see `ARCHITECTURE.md` §3.12); the DAG's `dagrun_timeout` and
  the operator's `waiter_max_attempts` are sized with headroom for that.
- Validated by actually loading this DAG against a real
  `apache-airflow==3.0.3` + `apache-airflow-providers-amazon` install — not
  just read from the source.
