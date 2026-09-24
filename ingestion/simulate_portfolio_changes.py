# Simula uma rodada de mudanças na hierarquia de carteira de clientes,
# reescrevendo o snapshot completo em
# ingestion/seed_data/bronze/portfolios/portfolios.csv e subindo pro S3 —
# de onde a dimensão SCD Tipo 2 dbt/models/silver/dim_client_portfolio.sql
# lê e, comparando com o snapshot anterior, fecha as versões que mudaram e
# abre as novas (ver dbt/macros/incremental_strategy_scd2_merge.sql).
#
# Dois tipos de mudança, que é o que de fato acontece numa carteira:
#   --reassign-pct  clientes que trocam de carteira (mudança "de baixo")
#   --rehome-pct    carteiras que trocam de gerente/região (mudança "de
#                   cima": afeta de uma vez todos os clientes da carteira)
#
# Diferente de simulate_cdc.py, aqui NÃO existe lote incremental: a fonte é
# um snapshot completo e o arquivo é *substituído* (mesmo nome, mesma key no
# S3), nunca acumulado. Subir um segundo arquivo ao lado do primeiro faria o
# glob bronze/portfolios/*.csv devolver o cliente duas vezes e abrir duas
# versões correntes pra ele.
#
# Uso:
#   python ingestion/generate_seed_data.py          # uma vez, se ainda não rodou
#   python ingestion/simulate_portfolio_changes.py
#   python ingestion/simulate_portfolio_changes.py --reassign-pct 5 --rehome-pct 10
#
# Depois de cada run, `dbt build --target prod --select dim_client_portfolio+`
# aplica as mudanças e propaga pro gold.
import argparse
import logging
import os
from pathlib import Path

import boto3
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("simulate_portfolio_changes")

SNAPSHOT_PATH = Path(__file__).parent / "seed_data" / "bronze" / "portfolios" / "portfolios.csv"
S3_KEY = "bronze/portfolios/portfolios.csv"


def mutate(con: duckdb.DuckDBPyConnection, reassign_pct: float, rehome_pct: float) -> dict:
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError(
            f"{SNAPSHOT_PATH} não encontrado — rode `python ingestion/generate_seed_data.py` "
            f"primeiro pra criar o snapshot inicial da hierarquia."
        )

    con.execute(f"CREATE OR REPLACE TABLE before AS SELECT * FROM read_csv_auto('{SNAPSHOT_PATH}')")

    # Catálogo das carteiras existentes, pra que um cliente reatribuído caia
    # numa carteira real (com gerente/região coerentes) em vez de num id solto.
    #
    # Exatamente uma linha por portfolio_id, via QUALIFY — um `SELECT DISTINCT`
    # das colunas não basta: se o snapshot trouxer a mesma carteira com
    # atributos divergentes (drift de origem, ou um snapshot montado à mão),
    # o DISTINCT devolve várias linhas pro mesmo id e o join lá embaixo abre
    # em leque, duplicando clientes no snapshot de saída — o que abriria duas
    # versões vigentes por cliente na SCD2. Pego exatamente assim num teste
    # com snapshot malformado.
    con.execute("""
        CREATE OR REPLACE TABLE portfolio_dim AS
        SELECT portfolio_id, portfolio_name, manager_id, manager_name, region, tier
        FROM before
        QUALIFY row_number() OVER (PARTITION BY portfolio_id ORDER BY customer_id) = 1
    """)
    n_portfolios = con.execute("SELECT count(*) FROM portfolio_dim").fetchone()[0]

    # Mudança "de cima": algumas carteiras inteiras trocam de gerente e região.
    # Um único sorteio por carteira, pra que todos os clientes dela recebam a
    # mesma mudança (senão a carteira ficaria com gerentes diferentes por
    # cliente, que não é o que acontece de verdade).
    con.execute(f"""
        CREATE OR REPLACE TABLE portfolio_dim_after AS
        SELECT
            portfolio_id,
            portfolio_name,
            CASE WHEN rehome THEN 1 + ((manager_id + 7) % 50) ELSE manager_id END AS manager_id,
            CASE WHEN rehome THEN 'Manager ' || (1 + ((manager_id + 7) % 50)) ELSE manager_name END AS manager_name,
            CASE WHEN rehome
                 THEN (['BR', 'US', 'PT', 'AR', 'MX'])[1 + (random() * 4)::INT]
                 ELSE region END AS region,
            tier
        FROM (
            SELECT *, random() < {rehome_pct / 100} AS rehome FROM portfolio_dim
        )
    """)

    # Índice denso sobre as carteiras que realmente existem, pra sortear uma
    # delas pelo índice. Sortear `1 + random()*(n-1)` como se fosse o próprio
    # portfolio_id assume que os ids são densos de 1..n, o que deixa de valer
    # assim que uma carteira perde todos os clientes e some do snapshot: o
    # sorteio passa a apontar pra id inexistente e o cliente é descartado no
    # join abaixo, encolhendo a população em silêncio. Pego pelo guard de
    # invariante no fim desta função, depois de 3 rodadas seguidas.
    con.execute("""
        CREATE OR REPLACE TABLE portfolio_pick AS
        SELECT row_number() OVER (ORDER BY portfolio_id) AS idx, portfolio_id
        FROM portfolio_dim
    """)

    # Mudança "de baixo": alguns clientes trocam de carteira.
    con.execute(f"""
        CREATE OR REPLACE TABLE assignment_after AS
        SELECT
            b.customer_id,
            CASE WHEN b.reassign THEN pick.portfolio_id ELSE b.portfolio_id END AS portfolio_id
        FROM (
            SELECT
                customer_id,
                portfolio_id,
                random() < {reassign_pct / 100} AS reassign,
                1 + (random() * ({n_portfolios} - 1))::INT AS pick_idx
            FROM before
        ) b
        LEFT JOIN portfolio_pick pick ON pick.idx = b.pick_idx
    """)

    con.execute("""
        CREATE OR REPLACE TABLE after AS
        SELECT
            a.customer_id,
            pd.portfolio_id,
            pd.portfolio_name,
            pd.manager_id,
            pd.manager_name,
            pd.region,
            pd.tier
        FROM assignment_after a
        JOIN portfolio_dim_after pd ON pd.portfolio_id = a.portfolio_id
    """)

    # Quantos clientes de fato mudam de atributo rastreado — é exatamente esse
    # o número de versões novas que a SCD2 deve abrir na próxima run.
    changed = con.execute("""
        SELECT count(*)
        FROM before b
        JOIN after a ON a.customer_id = b.customer_id
        WHERE (b.portfolio_id, b.manager_id, b.region, b.tier)
           IS DISTINCT FROM (a.portfolio_id, a.manager_id, a.region, a.tier)
    """).fetchone()[0]
    total = con.execute("SELECT count(*) FROM after").fetchone()[0]

    # Um snapshot completo tem que sair com exatamente os mesmos clientes com
    # que entrou — este script só muda atributos, nunca a população. Falhar
    # aqui é muito melhor do que gravar um snapshot duplicado e só descobrir
    # depois, como duas versões vigentes por cliente dentro da SCD2.
    before_customers = con.execute("SELECT count(DISTINCT customer_id) FROM before").fetchone()[0]
    after_customers = con.execute("SELECT count(DISTINCT customer_id) FROM after").fetchone()[0]
    if not (total == after_customers == before_customers):
        raise RuntimeError(
            f"snapshot de saída inconsistente: {total} linhas / {after_customers} clientes distintos, "
            f"contra {before_customers} clientes na entrada — esperado 1 linha por cliente."
        )

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY after TO '{SNAPSHOT_PATH}' (HEADER, DELIMITER ',')")

    return {"total": total, "changed": changed}


def upload(bucket: str) -> None:
    logger.info("Subindo %s para s3://%s/%s (substitui o snapshot anterior)", SNAPSHOT_PATH, bucket, S3_KEY)
    boto3.client("s3").upload_file(str(SNAPSHOT_PATH), bucket, S3_KEY)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reassign-pct", type=float, default=3.0, help="%% de clientes que trocam de carteira")
    parser.add_argument("--rehome-pct", type=float, default=5.0, help="%% de carteiras que trocam de gerente/região")
    parser.add_argument("--no-upload", action="store_true", help="Só reescreve o arquivo local, sem subir pro S3")
    args = parser.parse_args()

    con = duckdb.connect()
    stats = mutate(con, args.reassign_pct, args.rehome_pct)
    logger.info(
        "Snapshot reescrito: %s (%d clientes, %d com atributo rastreado alterado)",
        SNAPSHOT_PATH, stats["total"], stats["changed"],
    )

    bucket = os.getenv("LANDING_BUCKET")
    if args.no_upload:
        return
    if bucket:
        upload(bucket)
    else:
        logger.info("LANDING_BUCKET não definido — pulando upload. Suba manualmente quando quiser:")
        logger.info("  aws s3 cp %s s3://<landing-bucket>/%s", SNAPSHOT_PATH, S3_KEY)


if __name__ == "__main__":
    main()
