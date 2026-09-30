# Como acionar o rename manualmente

## Opção A — script local, sem Lambda nem S3

`rename_tables.py` roda direto na sua máquina, lendo o CSV de qualquer
pasta local — não precisa subir nada nem acionar a Lambda:

```bash
python -m table_admin.rename_tables \
  --csv ./minha-pasta/renames.csv \
  --table-bucket-arn arn:aws:s3tables:us-east-1:770724966330:bucket/dbt-duckdb-tables-770724966330
```

Parâmetros:

| Parâmetro | Valor |
|---|---|
| `--csv` | caminho do arquivo na sua máquina |
| `--table-bucket-arn` | ARN padrão — só usado nas linhas do CSV que não tiverem sua própria coluna `table_bucket_arn` |

Precisa só de credenciais AWS configuradas localmente (`aws configure`) e
`boto3` instalado (`pip install boto3`). Use `--dry-run` pra validar sem
executar nada. O formato do CSV é o mesmo da Opção B, mais abaixo.

## Opção B — via Lambda (acionamento manual)

### 1. Suba o CSV pro bucket que você quer usar

```bash
aws s3 cp renames.csv s3://<SEU-BUCKET>/table-renames/renames.csv
```

### 2. Invoque a Lambda apontando pra esse bucket/arquivo

```bash
aws lambda invoke \
  --function-name aws-duckdb-lakehouse-dev-table-rename \
  --cli-binary-format raw-in-base64-out \
  --payload '{"Records":[{"s3":{"bucket":{"name":"<SEU-BUCKET>"},"object":{"key":"table-renames/renames.csv"}}}]}' \
  response.json
```

Parâmetros que você ajusta:

| Parâmetro | Onde fica | Valor |
|---|---|---|
| Bucket do CSV | `bucket.name` no `--payload` | qualquer bucket desta conta AWS (não precisa ser o de landing) |
| Nome/caminho do CSV | `object.key` no `--payload` | o caminho onde você subiu o arquivo no passo 1 |
| Table bucket (onde renomear) | coluna `table_bucket_arn` no CSV | só se quiser renomear num S3 Tables bucket diferente do padrão — pegue o ARN com `aws s3tables list-table-buckets` |

### 3. Confira o resultado

```bash
aws s3 cp s3://<SEU-BUCKET>/table-renames/results/renames.csv.json -
```

## Formato do CSV

```csv
namespace,name,new_namespace,new_name,table_bucket_arn
silver,pedidos_antigo,,orders,
```

- `namespace`, `name`: obrigatórios — onde a tabela está hoje.
- `new_namespace`: só preenche se quiser mudar de namespace.
- `new_name`: só preenche se quiser mudar de nome.
- `table_bucket_arn`: só preenche se essa linha for renomear num table
  bucket diferente do padrão (`dbt-duckdb-tables-770724966330`).
