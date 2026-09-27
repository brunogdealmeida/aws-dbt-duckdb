# Quack on demand

Ad-hoc SQL against the lakehouse (silver + gold), executed by an ephemeral
ECS Fargate task per query (same image/task definition the dbt pipeline
uses, `MODE=query`) — no warehouse sits around idle. Full design/validation
notes: `ARCHITECTURE.md` §9.

## Local (UI, Postgres-backed history + saved queries)

```bash
cp .env.example .env   # fill in from `terraform output` in infra/
docker compose up -d
```

Open http://localhost:8000 — type SQL, hit "Executar", the result renders
as a table (first 500 rows; the full result is always downloadable as
`.parquet`). "Salvar query" names the current SQL for one-click reruns;
"Histórico" lists past runs, click one to reload its result.

Or the same thing via `curl`:
```bash
curl -X POST http://localhost:8000/queries -H 'Content-Type: application/json' \
  -d '{"sql": "select region, count(*) from fct_portfolio_revenue group by 1"}'
curl http://localhost:8000/queries/<job_id>
curl http://localhost:8000/queries              # history
```

Swagger UI: http://localhost:8000/docs

## Directly against AWS (no Postgres)

```bash
API_URL=$(terraform -chdir=../infra output -raw query_api_invoke_url)
curl -X POST "$API_URL/queries" -H 'Content-Type: application/json' -d '{"sql": "select 1"}'
```

Only `SELECT`/`WITH` statements are accepted — see `common.py`.
