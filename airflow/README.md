# Airflow DAG for this project

`dags/dbt_lakehouse_dag.py` triggers the **same** ECS Fargate task this
project already uses for EventBridge Scheduler and the manual GitHub
Actions dispatch (`MODE=dbt-build` on the `aws-duckdb-lakehouse-dev` task
definition). Airflow doesn't run dbt itself — it calls `ecs:RunTask` and
waits, reusing the Docker image CI/CD already builds and tests. That keeps
the dbt/DuckDB/Iceberg toolchain and S3 Tables credentials in one place
instead of duplicating them into the Airflow environment too.

Three ways to run it, in the order you'll probably want to try them:

- **§A** — your own existing Airflow instance, if you have one.
- **§B** — a full Airflow cluster in Docker, set up in this repo. **Recommended**
  if you're on macOS and don't already have an instance — verified working
  end to end (see §B's note on why §C isn't reliable on macOS).
- **§C** — a plain-Python (`venv`, no Docker) persistent local instance.
  Simpler, but hits a real macOS bug when Airflow's scheduler actually
  dispatches a task — see "Debugging notes" at the bottom before using this.

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

## B. Docker (recommended — verified end to end)

`docker-compose.yaml` in this folder is a trimmed-down version of the
[official Airflow 3.0.3 compose file](https://airflow.apache.org/docs/apache-airflow/3.0.3/docker-compose.yaml)
— `LocalExecutor` instead of `CeleryExecutor`, so no Redis/celery-worker/
flower (this project only ever runs one task at a time). Runs on real Linux
inside Docker, which matters — see "Debugging notes" below.

```bash
cd airflow
docker compose up airflow-init   # first time only: db migrate + create admin user
docker compose up -d             # start everything
docker compose down              # stop (add -v to also wipe the metadata db)
```

- **UI**: http://localhost:8080 — user/password `airflow`/`airflow` (set via
  `_AIRFLOW_WWW_USER_USERNAME`/`_AIRFLOW_WWW_USER_PASSWORD` in the compose
  file if you want something else).
- **AWS credentials**: `~/.aws` is mounted read-only into every container
  (`AWS_CONN_ID = None` in the DAG uses boto3's default chain) — same
  identity as `aws sts get-caller-identity` on your host, e.g.
  `terraform-admin`.
- The DAG is read directly from `dags/` (bind-mounted) — edit the file,
  it's picked up automatically (the `airflow-dag-processor` service polls
  for changes).
- `apache-airflow-providers-amazon` installs via `_PIP_ADDITIONAL_REQUIREMENTS`
  at container startup (a few extra seconds on first boot / after
  `docker compose down` — it's not baked into the image). Fine for this
  dev setup; see the upstream compose file's own comment if you want to
  bake it into a custom image instead.
- Logs: `docker-logs/` on the host (bind-mounted from the containers'
  `/opt/airflow/logs`), or in the UI under DAGs → `dbt_lakehouse` → the run
  → `dbt_build` → Logs. `docker-logs/` and `docker-plugins/` are gitignored.
- New DAGs start **paused** by default — `docker compose exec
  airflow-scheduler airflow dags unpause dbt_lakehouse` (or toggle it in the
  UI) if you want the schedule active; manual triggers work regardless of
  pause state.

## C. Persistent local instance (plain `venv`, no Docker)

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
- **AWS credentials**: uses your local default boto3 chain, same as §B.
- The DAG is read directly from `airflow/dags/` (via
  `AIRFLOW__CORE__DAGS_FOLDER`, set by `start.sh`) — no copying needed.
- **On macOS, real scheduled runs (not `airflow dags test`) hang** — see
  "Debugging notes" below. `start.sh` sets a couple of env vars that reduce
  but don't eliminate the odds of hitting it. Use §B if you hit this.

## Notes

- **This does not replace** the EventBridge Scheduler (`infra/scheduler.tf`,
  daily 03:00 UTC) — the two can coexist; they both just call `ecs:RunTask`
  on the same task definition. If you want Airflow to own scheduling
  instead, set `schedule=None` in the DAG to a cron expression and disable
  (or remove) `infra/scheduler.tf`'s schedule to avoid double-running.
- The 5M-row `stg_orders` model alone takes anywhere from ~10 seconds to
  ~9 minutes to write, depending on S3/Iceberg REST catalog latency that
  varies run to run (see `ARCHITECTURE.md` §3.12) — the DAG's
  `dagrun_timeout` and the operator's `waiter_max_attempts` are sized with
  headroom for the slow end of that range.

## Debugging notes

Both found by actually running this, not by reading the source — kept here
in case you hit either one again (e.g. after an Airflow upgrade).

**macOS + Airflow's `LocalExecutor` hangs indefinitely on real (scheduler-
dispatched) task runs.** `airflow dags test` (which runs the whole DAG
in-process, no fork/subprocess) worked fine every time; a task actually
queued and dispatched by the scheduler consistently hung after resolving
AWS credentials and before the `ecs:RunTask` call ever fired — 90-100% CPU,
zero network activity, indefinitely. Stack-sampling the stuck process
(`sample <pid>` on macOS, `py-spy dump --pid <pid>` needs `sudo` there)
showed it spinning in `getaddrinfo`/DNS resolution C frames. Tried and
**none of these fixed it**: `NO_PROXY=*`/`no_proxy=*` (rules out macOS's
non-fork-safe `SCDynamicStoreCopyProxiesWithOptions` proxy lookup),
`OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` (the standard Objective-C
fork-safety workaround), and explicitly forcing
`AIRFLOW__CORE__MP_START_METHOD=spawn` (Airflow's `LocalExecutor` already
uses `multiprocessing.Process()`, which should default to spawn on macOS
anyway). Root cause not fully identified — moved to Docker (§B), which
runs on Linux and doesn't exhibit this at all, rather than keep digging.

**Airflow 3.0.x docker-compose: `Invalid auth token: Signature
verification failed`.** With multiple containers (scheduler, api-server,
...) and no `AIRFLOW__API_AUTH__JWT_SECRET` set, each one generates its own
random JWT signing key at startup, so the scheduler-signed task auth token
fails the api-server's signature check on every single task — a known gap
in Airflow 3.0.x ([apache/airflow#59373](https://github.com/apache/airflow/issues/59373)).
Fixed by setting a shared static `AIRFLOW__API_AUTH__JWT_SECRET` in
`docker-compose.yaml`'s common environment block.
