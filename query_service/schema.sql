-- Metadados do "quack on demand": histórico de execução (mirror local do
-- status.json que ingestion/query_runner.py escreve no S3 — ver a nota em
-- lambda_submit.py sobre por que a fonte de verdade em produção é o S3, não
-- esse Postgres) e a biblioteca de queries salvas.
--
-- Aplicado automaticamente pelo docker-compose (postgres monta
-- /docker-entrypoint-initdb.d na primeira subida do volume).

CREATE TABLE IF NOT EXISTS executions (
    job_id       TEXT PRIMARY KEY,
    sql          TEXT NOT NULL,
    status       TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    row_count    BIGINT,
    result_key   TEXT,
    error        TEXT
);

CREATE INDEX IF NOT EXISTS executions_submitted_at_idx ON executions (submitted_at DESC);

CREATE TABLE IF NOT EXISTS saved_queries (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    sql        TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
