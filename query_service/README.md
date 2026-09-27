# Quack on demand

Ad-hoc SQL against the lakehouse (silver + gold), executed by an ephemeral
ECS Fargate task per query (same image/task definition the dbt pipeline
uses, `MODE=query`) — no warehouse sits around idle. Full design/validation
notes: `ARCHITECTURE.md` §9.

## Local (Postgres-backed history + saved queries)

```bash
cp .env.example .env   # fill in from `terraform output` in infra/
docker compose up -d
```

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
