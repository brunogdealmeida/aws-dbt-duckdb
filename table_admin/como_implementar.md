# Como usar o rename de tabelas em outra conta AWS

Guia de onde mexer pra levar `table_admin/` (script + Lambda de rename em
lote do S3 Tables) pra uma conta AWS diferente desta. Design completo e
histórico de bugs/decisões: `ARCHITECTURE.md` §10.

Tem dois jeitos de usar essa ferramenta, e eles precisam de coisas
diferentes pra funcionar numa conta nova:

| Jeito | Precisa implantar Terraform? | O que muda por conta |
|---|---|---|
| CLI local (`rename_tables.py`) | Não | Só as credenciais AWS e o ARN que você passa na hora |
| Lambda (`lambda_handler.py`, automática ou manual) | Sim | Variáveis do `infra/terraform.tfvars` |

## 1. Só o CLI, sem implantar nada

O caminho mais simples pra usar isso numa conta nova — não depende do
Terraform deste projeto rodar lá. Só precisa de:

1. Credenciais AWS da conta de destino configuradas (`aws configure`,
   variáveis `AWS_*`, ou um profile — o script usa a cadeia padrão de
   credenciais do boto3, igual o resto do projeto).
2. O ARN do table bucket de destino, no formato:
   ```
   arn:aws:s3tables:<região>:<account-id>:bucket/<nome-do-bucket>
   ```
   Pega esse valor com `aws s3tables list-table-buckets` na conta de
   destino, ou — se esse projeto Terraform também estiver implantado
   lá — com `terraform -chdir=infra output -raw s3_tables_bucket_arn`.
3. Rodar normalmente:
   ```bash
   python -m table_admin.rename_tables \
     --csv renames.csv \
     --table-bucket-arn arn:aws:s3tables:us-east-1:<OUTRA-CONTA>:bucket/<bucket-de-destino> \
     --dry-run
   ```

Nenhum arquivo do projeto precisa ser editado pra isso — `--table-bucket-arn`
(ou a coluna `table_bucket_arn` do CSV, ver §10.7 do ARCHITECTURE.md) já é
um parâmetro, não tem nada hardcoded no script.

## 2. Implantando a Lambda na conta nova

Se você quer o gatilho automático (CSV cai em `table-renames/`, a Lambda
renomeia sozinha) ou a invocação manual sem precisar exportar credenciais
toda vez, precisa aplicar o Terraform deste projeto na conta nova.
**Atenção:** `infra/` é um módulo Terraform só — não dá pra aplicar
`table_admin.tf` isolado, `terraform apply` sobe o projeto inteiro (ECS,
ECR, Athena, S3 Tables, o resto do "quack on demand", etc.), não só o
rename tool. Se você só quer essa ferramenta numa conta nova sem o resto
do lakehouse, o caminho 1 (só o CLI) é o que serve — implantar a Lambda
exige o restante da infra junto, porque `table_admin.tf` referencia
`aws_s3_bucket.landing` e `aws_s3tables_table_bucket.lakehouse`, definidos
em `s3.tf`/`s3tables.tf`.

Como `infra/table_admin.tf` não tem **nenhum** valor hardcoded — tudo vem de
variável ou de outro recurso do Terraform — não tem nada pra editar
*nesse arquivo*. O que muda é só o `infra/terraform.tfvars` (arquivo
local, fora do git — comece copiando `infra/terraform.tfvars.example`):

| O que mudar | Variável | Onde é usada por `table_admin.tf` |
|---|---|---|
| Conta/credenciais | (via `aws configure`/OIDC) | Tudo — Terraform aplica na conta que suas credenciais apontam |
| Região | `aws_region` | Entra no ARN do table bucket via `aws_s3tables_table_bucket.lakehouse.arn` |
| Bucket de landing (onde o CSV cai, gatilho automático) | `landing_bucket_name` | `aws_s3_bucket_notification.landing`, `aws_lambda_permission.table_admin_rename_s3_invoke` |
| Bucket do S3 Tables (onde as tabelas moram) | `s3_tables_bucket_name` | Vira `S3_TABLE_BUCKET_ARN` (env var da Lambda, o *padrão* quando o CSV não especifica `table_bucket_arn` — ver §10.7) |
| Nome do projeto / ambiente | `project_name`, `environment` | Nome da função (`<project_name>-<environment>-table-rename`), nome da IAM role |

Numa conta que nunca rodou este Terraform, o bucket de state remoto
precisa existir primeiro — isso é o bootstrap (`infra/bootstrap/`, rodado
uma única vez; detalhes em `ARCHITECTURE.md` §2.1). Depois disso, e com o
`terraform.tfvars` editado:
```bash
cd infra
cp backend.hcl.example backend.hcl   # state remoto também é por conta — edite antes do init
terraform init -backend-config=backend.hcl
terraform plan -var-file=terraform.tfvars
terraform apply
```

`terraform output` depois do apply já te dá os valores certos pra usar,
sem digitar ARN à mão:
```bash
terraform output -raw s3_tables_bucket_arn      # pro --table-bucket-arn do CLI
terraform output -raw table_rename_upload_prefix  # onde subir o CSV pro gatilho automático
```

O nome da função Lambda segue o padrão `<project_name>-<environment>-table-rename`
— se você não mudar `project_name`/`environment` no `tfvars`, fica
`aws-duckdb-lakehouse-dev-table-rename` (os valores default), igual esta
conta. Pra invocar manualmente:
```bash
aws lambda invoke \
  --function-name <project_name>-<environment>-table-rename \
  --cli-binary-format raw-in-base64-out \
  --payload '{"Records":[{"s3":{"bucket":{"name":"<qualquer-bucket-dessa-conta>"},"object":{"key":"table-renames/renames.csv"}}}]}' \
  response.json
```

## 3. O coringa de bucket (§10.8) só vale dentro da mesma conta

A IAM role da Lambda tem `s3:GetObject`/`s3:PutObject` em
`arn:aws:s3:::*/table-renames/*` — **qualquer bucket, mas só dentro da
conta AWS onde essa Lambda está implantada**. Isso não dá acesso a
buckets de outras contas: IAM é por conta, um coringa `*` no ARN nunca
atravessa fronteira de conta. Se você quer que a Lambda leia um CSV que
está numa **conta diferente** da que ela roda, isso é outra coisa —
precisaria de uma bucket policy no bucket de origem liberando a role
dessa Lambda (`aws-duckdb-lakehouse-dev-table-admin-lambda` ou o nome
equivalente na conta nova) como principal, ou de uma role assumível
cross-account. Não implementado aqui — fora do escopo original do pedido
(ver ARCHITECTURE.md §10.8).

## Checklist rápido

- [ ] Só CLI, sem deploy? → só precisa das credenciais da conta nova + o
      ARN certo em `--table-bucket-arn` (ou na coluna `table_bucket_arn`
      do CSV).
- [ ] Quer a Lambda implantada lá? → copie `infra/terraform.tfvars.example`
      pra `infra/terraform.tfvars`, preencha `landing_bucket_name`,
      `s3_tables_bucket_name`, `aws_region` (e `project_name`/`environment`
      se quiser nomes diferentes), rode `terraform apply` com credenciais
      da conta nova.
- [ ] Não edite `infra/table_admin.tf` nem `table_admin/*.py` — nenhum dos
      dois tem valor específico de conta hardcoded.
- [ ] Buckets de origem do CSV em contas **diferentes** da que roda a
      Lambda não são cobertos por essa ferramenta hoje (§3 acima).
