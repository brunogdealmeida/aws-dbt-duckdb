# Como acionar o rename manualmente

## 1. Suba o CSV pro bucket que você quer usar

```bash
aws s3 cp renames.csv s3://<SEU-BUCKET>/table-renames/renames.csv
```

## 2. Invoque a Lambda apontando pra esse bucket/arquivo

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

## 3. Confira o resultado

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
