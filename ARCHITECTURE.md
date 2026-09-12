# Arquitetura — AWS - Datalab Project (dbt + DuckDB + S3 Tables + Athena)

Este documento descreve a arquitetura completa do projeto, o que foi
criado na AWS, os problemas encontrados ao subir a stack (e
como foi resolvido), como renomear os recursos principais, e como rodar o
pipeline local e na AWS.

---

## 1. Arquitetura

### 1.1 Fluxo de dados

```text
ingestion/ingest_csv.py (ou generate_seed_data.py)
            |
            v
   s3://<landing-bucket>/bronze/<entidade>/*.csv       <- camada bronze (raw)
            |
            v
   dbt source `bronze.<entidade>`
   (lido via httpfs do DuckDB, sem catálogo — direto do S3)
            |
            v
   dbt/models/silver/{stg_orders,stg_clients,stg_inventory}.sql
   (cast de tipos, normalização de texto, filtro de PK nula)
            |
            v
   DuckDB ATTACH ... (TYPE iceberg, ENDPOINT_TYPE s3_tables)
            |
            v
   S3 Tables — bucket dedicado, namespace `silver` (formato Iceberg)  <- camada silver
            |
            v
   Federação Glue Data Catalog (s3tablescatalog) + Lake Formation
            |
            v
   Athena (data source `datalab-duckdb`)
```

As três entidades da camada silver têm integridade referencial de verdade pra usar em uma futura camada
gold:

```text
stg_orders.customer_id -> stg_clients.customer_id
stg_orders.product_id  -> stg_inventory.product_id
stg_orders.amount        é derivado de stg_inventory.unit_cost * quantity (com ruído)
```

### 1.2 Execução / orquestração

```text
GitHub push em main
       |
       v
  CI (dbt parse/compile no target `dev`, build da imagem Docker)
       |
       v
  Terraform (plan em PR; plan+apply em push, se infra/** mudou)
       |
       v
  Deploy Lakehouse (build+push da imagem pro ECR, registra nova revisão
  da task definition do ECS)
       |
       v
  Execução do dbt:
    - manual: workflow_dispatch no GitHub Actions, ou `aws ecs run-task`
    - automática: EventBridge Scheduler, todo dia às 03:00 UTC
```

A task do ECS é **efêmera**: sobe, roda `dbt build`/`dbt run`/`ingest`
conforme a variável `MODE`, escreve no S3 Tables, e morre. Não há nada
rodando 24/7 (ver §5 sobre por que não usamos Airflow/MWAA aqui).

### 1.3 Autenticação / segurança

- GitHub Actions autentica na AWS via **OIDC** (sem chaves de longa duração)
  — role `github-actions-lakehouse-deploy`, usada tanto pra build/deploy
  quanto pra rodar `terraform apply`.
- A task do ECS recebe permissões via **Task Role** (`ecs_task`), também sem
  chaves embutidas — usa a cadeia de credenciais do DuckDB
  (`PROVIDER credential_chain`) que pega automaticamente as credenciais do
  papel da task.
- Localmente, um usuário IAM dedicado (`terraform-admin`, com
  `AdministratorAccess`) é usado para bootstrap e para debug manual — a
  conta não usa mais a access key de root.

---

## 2. O que foi criado

### 2.1 Terraform — bootstrap (`infra/bootstrap/`)

Aplicado uma única vez, manualmente, com state local (resolve o problema de
"a config não pode referenciar o próprio backend que ela cria"):

| Recurso | Propósito |
|---|---|
| `aws_s3_bucket.tfstate` | Bucket do state remoto do Terraform |
| `aws_dynamodb_table.tfstate_lock` | Lock table (PAY_PER_REQUEST) |

### 2.2 Terraform — infraestrutura principal (`infra/`)

| Arquivo | Recursos | Propósito |
|---|---|---|
| `versions.tf` / `backend.tf` | providers `aws`+`awscc`, backend S3 | Config base |
| `main.tf` | ECR, cluster ECS, task definition Fargate, log group, IAM roles (`ecs_execution`, `ecs_task`, `github_deploy`), OIDC provider do GitHub | Compute + deploy |
| `s3.tf` | `aws_s3_bucket.landing` (+ versionamento, criptografia, bloqueio público) | Camada bronze |
| `s3tables.tf` | `aws_s3tables_table_bucket`, `aws_s3tables_namespace.silver` | Camada silver (Iceberg) |
| `glue_lakeformation.tf` | Role de federação, `aws_lakeformation_resource`, `aws_lakeformation_data_lake_settings`, `awscc_glue_catalog.s3tables`, `aws_lakeformation_permissions` | Torna o S3 Tables visível/consultável pelo Glue + Lake Formation |
| `athena.tf` | Bucket de resultados, `aws_athena_workgroup`, `aws_athena_data_catalog` (nome: `datalab-duckdb`) | Consulta via Athena |
| `scheduler.tf` | Role do EventBridge Scheduler, `aws_scheduler_schedule.dbt_build` | Roda `dbt build` todo dia às 03:00 UTC |
| `outputs.tf` | Nomes/ARNs de tudo acima | Referência rápida (`terraform output`) |

### 2.3 dbt (`dbt/`)

| Arquivo | Propósito |
|---|---|
| `profiles.yml` | Target `dev` (100% local, sem AWS) e `prod` (ECS, anexa o S3 Tables via `attach`/`secrets`) |
| `dbt_project.yml` | Config condicional por target: materialização, `database`, `schema` |
| `models/sources.yml` | Fontes bronze (`orders`, `clients`, `inventory`), lidas via `external_location` |
| `models/silver/*.sql` | Transformações (cast, normalização, filtro de PK) |
| `macros/generate_schema_name.sql` | Faz o schema resolver pra `silver` (não `main_silver`, que é o padrão do dbt) |
| `macros/materialization_iceberg_table.sql` | Materialização customizada pro target `prod` (ver §3.7) |

### 2.4 Ingestão (`ingestion/`)

| Arquivo | Propósito |
|---|---|
| `ingest_csv.py` | Sobe CSVs locais pro landing bucket (`bronze/<source>/`) |
| `generate_seed_data.py` | Gera dados de teste em volume real via DuckDB (orders=5M, clients=100k, inventory=1M linhas), com FKs íntegras |
| `sample_data/*.csv` | Amostras pequenas e FK-consistentes, usadas pelo target `dev`/CI |

### 2.5 CI/CD (`.github/workflows/`)

| Workflow | Gatilho | O que faz |
|---|---|---|
| `ci.yml` | push/PR em `main` | `dbt parse`/`compile` (target `dev`), build da imagem Docker |
| `terraform.yml` | push/PR tocando `infra/**` | `plan` (sempre) + `apply` (só em push a `main`) |
| `deploy.yml` | push em `main`, ou manual | build+push da imagem pro ECR, registra task definition, roda task (se manual) |

---

## 3. Problemas encontrados e como foram resolvidos

Esta seção documenta **cada bug real** descoberto rodando de verdade contra
AWS/GitHub — nenhum deles aparece só lendo o código.

### 3.1 Build da imagem Docker quebrado

**Sintoma:** `docker build` falhava sempre, com qualquer contexto.
**Causa:** o `Dockerfile` original tinha `COPY` incompatível com qualquer
diretório de contexto (esperava `dbt/dbt_project.yml` E `ingestion/` como se
estivessem no mesmo nível, o que nunca é verdade).
**Correção:** contexto de build = raiz do repo; `COPY dbt/dbt_project.yml
dbt/profiles.yml ./`, `COPY dbt/models ./models`, `COPY ingestion
./ingestion`. CI/deploy usam `docker build -f dbt/Dockerfile .`.

### 3.2 `model-paths` errado no dbt

**Sintoma:** warning "unused configuration path" e, depois de corrigir o
Dockerfile, os models simplesmente não eram encontrados.
**Causa:** `model-paths: ["dbt/models"]` — mas `dbt_project.yml` já está
dentro da pasta `dbt/`, então o caminho correto (relativo a ele) é só
`models`.
**Correção:** `model-paths: ["models"]`, `macro-paths: ["macros"]`.

### 3.3 `dbt-core==1.11.0` yanked no PyPI

**Sintoma:** `pip install` emite warning de "yanked version" (removida por
bug de bounds de dependência).
**Correção:** atualizado para `dbt-core==1.11.8`.

### 3.4 Nome de bucket do S3 Tables não pode começar com `aws`

**Sintoma:** `terraform apply` falha com `BadRequestException: The
specified bucket name isn't valid`.
**Causa:** regra de nomenclatura específica do S3 Tables (diferente de S3
comum): nomes não podem começar com `aws`, `xn--`, `sthree-`,
`amzn-s3-demo-`.
**Correção:** renomeado de `aws-dbt-duckdb-tables-*` para
`dbt-duckdb-tables-*`.

### 3.5 `for_each` com atributo de recurso ainda não criado

**Sintoma:** `terraform plan` falha: "The for_each set includes values
derived from resource attributes that cannot be determined until apply".
**Causa:** `aws_lakeformation_permissions.silver_readers` usava
`for_each = toset(concat([aws_iam_role.ecs_task.arn], ...))` — o ARN da
role não existe ainda num apply do zero, e `for_each` exige que todas as
chaves sejam conhecidas em tempo de plan.
**Correção:** trocado para `count = length(lista)`, que só precisa saber o
*tamanho* da lista (isso sim é conhecido estaticamente).

### 3.6 Trust policy do OIDC do GitHub incompleta (dois bugs em sequência)

**Sintoma 1:** `AssumeRoleWithWebIdentity: Not authorized`.
**Causa 1:** os jobs usam `environment: dev`, o que muda o formato do claim
`sub` do token OIDC de `repo:OWNER/REPO:ref:refs/heads/main` para
`repo:OWNER/REPO:environment:dev` — e a trust policy só aceitava o primeiro
formato.
**Correção 1:** aceitar os dois formatos via `StringLike` com múltiplos
valores.

**Sintoma 2:** o erro persistiu mesmo depois da correção acima.
**Causa 2:** descoberta rodando um passo de debug temporário que decodifica
o JWT: o `sub` real era
`repo:brunogdealmeida@59927344/aws-dbt-duckdb@1367242709:environment:dev`
— o GitHub anexa um **ID numérico imutável** ao owner e ao repo (proteção
contra sequestro de trust por rename), algo que não aparece nos exemplos
oficiais de documentação.
**Correção 2:** wildcard no formato: `repo:OWNER*/REPO*:environment:dev`,
casando com ou sem o sufixo `@id`.

### 3.7 Role do GitHub Actions sem permissão pra rodar Terraform

**Sintoma:** `terraform init` falha com 403 ao acessar o bucket de state; ou
falhas de permissão ao criar recursos.
**Causa:** a role `github-actions-lakehouse-deploy` só tinha permissões pra
ECR/ECS (desenhada originalmente só pra build+deploy de imagem), mas também
passou a rodar `terraform apply` completo.
**Correção:** anexado `AdministratorAccess` (opção escolhida
deliberadamente pra dev, ver comentário em `main.tf` sobre restringir em
prod).

### 3.8 Lake Formation exige registro de admin separado do IAM

**Sintoma:** `AccessDeniedException` ao rodar `aws_lakeformation_permissions`
mesmo com `AdministratorAccess`.
**Causa:** Lake Formation tem uma camada de permissão própria, separada do
IAM — só principals registrados como "data lake admin"
(`aws_lakeformation_data_lake_settings`) podem chamar `GrantPermissions`.
**Correção:** adicionada a role do GitHub Actions à lista de admins.

### 3.9 Hook `on-run-start` pra anexar o S3 Tables nunca funcionava

**Sintoma:** `Binder Error: Catalog "s3_tables" does not exist!`, mesmo com
o hook rodando `ATTACH ...` antes dos models.
**Causa:** descoberta rodando `dbt --debug`: o dbt-duckdb lista os schemas
existentes em todo catálogo referenciado por um model (aqui, `s3_tables`)
**antes** de rodar qualquer hook — então a query de cache falha antes do
hook ter chance de anexar o catálogo.
**Correção:** mover `attach`/`secrets` pra dentro de `profiles.yml` (não um
hook) — isso é reaplicado pelo dbt-duckdb toda vez que ele abre uma conexão
nova, inclusive a interna usada pra esse cache.

### 3.10 Materialização padrão do dbt-duckdb incompatível com Iceberg

**Sintoma:** `Not implemented Error: Alter Schema Entry`.
**Causa:** a materialização `table` (e `incremental`) do dbt-duckdb cria uma
tabela intermediária e troca via `ALTER TABLE ... RENAME TO` — operação que
o catálogo Iceberg do DuckDB não implementa.
**Correção:** materialização customizada (`iceberg_table`) que faz
`DROP TABLE IF EXISTS` + `CREATE TABLE ... AS` (testado manualmente e
confirmado que funciona), usada só no target `prod`.

### 3.11 Config duplicada no model sobrescrevendo o `dbt_project.yml`

**Sintoma:** mesmo com a materialização customizada configurada no
`dbt_project.yml`, o `manifest.json` mostrava `materialized: table` (a
antiga, quebrada).
**Causa:** cada model `.sql` tinha `{{ config(materialized='table') }}`
no topo — config no nível do model tem prioridade sobre o projeto.
**Correção:** removido dos três models; a materialização agora só é
decidida pelo `dbt_project.yml` (condicional por target).

### 3.12 DuckDB 1.4.0 escreve Iceberg que o Athena não consegue ler

**Sintoma:** qualquer query no Athena contra as tabelas (mesmo
`SELECT * LIMIT 1`) falha com `GENERIC_INTERNAL_ERROR: Cannot invoke
"java.lang.Long.longValue()" because "value" is null`.
**Investigação:** confirmado que não é problema de configuração — mesmo uma
tabela criada pelo **próprio Athena**, ao receber um único `INSERT` do
DuckDB, passa a falhar do mesmo jeito. É um gap de compatibilidade real e
aberto entre o *writer* Iceberg do DuckDB e o *reader* do Trino/Athena
([duckdb/duckdb-iceberg#488](https://github.com/duckdb/duckdb-iceberg/issues/488)).
**Correção:** atualizado `DUCKDB_VERSION` de `1.4.0` para `1.5.5` (e
`dbt-duckdb` de `1.10.0` para `1.11.0`). Testado de ponta a ponta com os
6,1M de linhas reais — Athena passou a ler tudo corretamente, com bônus de
a escrita ficar **mais de 2x mais rápida** (orders: 18min → 8m45s).

### 3.13 Versão nova do DuckDB rejeita `CREATE` logo após `DROP` na mesma transação

**Sintoma (só depois do upgrade acima):** `Not implemented Error: Cannot
create table deleted within a transaction`.
**Causa:** a extensão iceberg mais nova não permite criar uma tabela com o
mesmo nome de uma deletada ainda dentro da mesma transação aberta.
**Correção:** `{{ adapter.commit() }}` explícito logo após o `DROP TABLE`,
antes do `CREATE TABLE`, na materialização customizada.

### 3.14 Federação do Glue não é suficiente pro Athena enxergar o catálogo

**Sintoma:** `SCHEMA_NOT_FOUND` ao consultar `s3tablescatalog.silver.orders`
no Athena, mesmo com o catálogo federado corretamente criado no Glue.
**Causa:** o Athena precisa de um registro **separado**, como "data
source" (`aws athena create-data-catalog` / `aws_athena_data_catalog`) —
isso não aparece em `list-data-catalogs` até ser feito.
**Correção adicional:** o `catalog-id` desse registro precisa incluir o
nome do table bucket (`<account>:s3tablescatalog/<bucket>`) — a forma sem o
bucket (`<account>:s3tablescatalog`) resolve mas não retorna nenhum
database. Automatizado via `aws_athena_data_catalog` no Terraform.

### 3.15 Scheduler apontando pra revisão travada da task definition

**Risco identificado (corrigido antes de virar bug):** se o EventBridge
Scheduler referenciasse `aws_ecs_task_definition.lakehouse.arn` diretamente,
ficaria preso pra sempre na revisão que o Terraform criou no bootstrap —
porque o `deploy.yml` registra novas revisões *fora* do Terraform
(`lifecycle { ignore_changes = [container_definitions] }` em `main.tf`).
**Correção:** o ARN usado no `target_definition_arn` do scheduler omite o
número de revisão (`.../task-definition/<family>`, sem `:N`), fazendo o
ECS sempre resolver pra última revisão ATIVA automaticamente.

### 3.16 `S3_TABLE_BUCKET` errado na task definition, escondido havia semanas

**Sintoma:** a primeira execução real do `dbt-build` via `aws ecs run-task`
depois de configurar o upload de logs (§6) falhou com
`Request to 's3tables.us-east-1.amazonaws.com/.../config?warehouse=...
bucket%2Faws-dbt-duckdb-tables-770724966330' ... NotFound_404` — note o
`aws-` no nome do bucket.
**Causa:** exatamente o mecanismo descrito em §6.3, mas dessa vez causando
um bug de verdade, não só um risco: a task definition foi criada pela
**primeira vez** (bootstrap) quando `s3_tables_bucket_name` no
`terraform.tfvars` ainda tinha o nome antigo (com `aws-`, antes da correção
do item 3.4). O bucket em si foi corrigido depois, mas o valor do
`S3_TABLE_BUCKET` ficou **congelado** na task definition por causa do
`ignore_changes` — nenhum dos `terraform apply` seguintes corrigiu isso, e
nenhum deploy via `deploy.yml` também (ele só troca a imagem). Passou
despercebido porque todo teste anterior contra o `prod` target rodou
localmente com as variáveis de ambiente setadas manualmente no terminal,
nunca lendo o valor real gravado na task definition do ECS.
**Correção:** registrada uma nova revisão (13) com `S3_TABLE_BUCKET`
corrigido, usando o mesmo processo do §6.3.
**Lição:** depois de qualquer rename de recurso referenciado por env var da
task (bucket, namespace...), **sempre** conferir o valor real via
`aws ecs describe-task-definition ... --query
"taskDefinition.containerDefinitions[0].environment"` — não basta o
`terraform apply` "não dar erro".

---

## 4. Como renomear buckets / namespace / catálogo do Athena

Todos os nomes são variáveis do Terraform (`infra/variables.tf`), definidas
em `infra/terraform.tfvars` (arquivo local, fora do git) e replicadas como
variáveis do GitHub Environment `dev` (usadas pelo `terraform.yml` pra
gerar o `tfvars.json` do CI).

| O que mudar | Variável Terraform | Variável GitHub |
|---|---|---|
| Bucket de landing (bronze) | `landing_bucket_name` | `LANDING_BUCKET_NAME` |
| Bucket do S3 Tables | `s3_tables_bucket_name` | `S3_TABLES_BUCKET_NAME` |
| Namespace (schema) da camada silver | `s3_tables_namespace` | `S3_TABLES_NAMESPACE` |
| Bucket de resultados do Athena | `athena_results_bucket_name` | `ATHENA_RESULTS_BUCKET_NAME` |
| **Nome do catálogo no Athena** | `athena_data_catalog_name` | `ATHENA_DATA_CATALOG_NAME` |

### Passo a passo

1. Edite `infra/terraform.tfvars` com o novo valor.
2. Atualize a variável correspondente no GitHub:
   ```bash
   gh variable set ATHENA_DATA_CATALOG_NAME --env dev --body "novo-nome" \
     -R brunogdealmeida/aws-dbt-duckdb
   ```
3. Aplique:
   ```bash
   cd infra
   terraform plan   # confira o que vai mudar
   terraform apply
   ```
   ou simplesmente dê `git push` — o workflow `terraform.yml` faz
   `plan`+`apply` automaticamente em push pra `main` (se algo em `infra/**`
   mudou).

### Atenção: nem toda renomeação é "de graça"

- **Buckets S3 (landing, athena results) e o table bucket do S3 Tables**:
  nome é imutável — o Terraform vai **destruir e recriar** o bucket. Se já
  tiver dados, isso significa perda de dados a menos que você migre antes
  (`aws s3 sync` / `aws s3tables` para o bucket novo).
- **`athena_data_catalog_name`**: seguro trocar — é só um "ponteiro"
  (registro no Athena), não guarda dado nenhum. Foi isso que fizemos ao
  renomear pra `datalab-duckdb` (destroy+recreate do registro, zero
  impacto nos dados).
- **`s3_tables_namespace`**: cuidado — muda o schema que o dbt usa
  (`+schema` em `dbt_project.yml` também lê `S3_TABLES_NAMESPACE`). Se
  mudar, as tabelas antigas continuam no namespace antigo; rode
  `dbt build --target prod` de novo pra criar as tabelas no namespace novo.

---

## 5. Como rodar o dbt

### 5.1 Local, sem AWS (target `dev`)

Roda 100% offline: lê `ingestion/sample_data/*.csv` (amostras pequenas,
FK-consistentes) e materializa num arquivo DuckDB local (`/tmp/dbt_dev.duckdb`).

```bash
cd dbt
pip install -r requirements.txt
DBT_PROFILES_DIR=. dbt build --target dev
```

Use isso para desenvolver/testar mudanças nos models rapidamente, sem
custo e sem depender de rede.

### 5.2 Local, contra a AWS real (target `prod`, rodando na sua máquina)

Útil para debugar problemas que só aparecem contra a infra real (foi assim
que os bugs da seção 3.9–3.13 foram encontrados e corrigidos).

```bash
cd dbt
pip install -r requirements.txt

export AWS_ACCOUNT_ID=770724966330
export AWS_REGION=us-east-1
export LANDING_BUCKET=aws-dbt-duckdb-landing-770724966330
export S3_TABLE_BUCKET=dbt-duckdb-tables-770724966330
export S3_TABLES_NAMESPACE=silver
export DBT_PROFILES_DIR=.

# precisa de credenciais AWS válidas no ambiente (aws configure / aws sso login)
dbt build --target prod
```

Para rodar só um model específico (útil pra não esperar os 5M de linhas do
`stg_orders` toda vez): `dbt build --target prod --select stg_clients stg_inventory`.

### 5.3 Na AWS, via ECS (produção)

**Opção A — automática:** já está configurada via EventBridge Scheduler
(`infra/scheduler.tf`), rodando `dbt build` todo dia às 03:00 UTC. Pra
mudar o horário, edite `dbt_build_schedule_expression` em
`infra/variables.tf` (ou passe via `-var` no apply) — é uma expressão cron
padrão do EventBridge (`cron(0 3 * * ? *)`).

**Opção B — manual, via GitHub Actions:**
GitHub → Actions → "Deploy Lakehouse" → Run workflow → escolha o modo
(`dbt-build`, `dbt-run`, `dbt-test`, `ingest`, `none`).

**Opção C — manual, via CLI:**
```bash
aws ecs run-task \
  --cluster aws-duckdb-lakehouse-dev \
  --task-definition aws-duckdb-lakehouse-dev \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[...],securityGroups=[...],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"lakehouse","environment":[{"name":"MODE","value":"dbt-build"}]}]}'
```
(subnets/security group: `terraform output` na pasta `infra/`, ou veja as
variáveis `ECS_SUBNETS`/`ECS_SECURITY_GROUPS` do GitHub Environment.)

### 5.4 Consultando o resultado

**Via DuckDB direto** (funciona igual ao que o `prod` target faz):
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; INSTALL aws; INSTALL iceberg; LOAD httpfs; LOAD aws; LOAD iceberg;")
con.execute("CREATE SECRET s3_tables_secret (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
con.execute("ATTACH IF NOT EXISTS 'arn:aws:s3tables:us-east-1:770724966330:bucket/dbt-duckdb-tables-770724966330' "
            "AS s3_tables (TYPE iceberg, ENDPOINT_TYPE s3_tables, SECRET s3_tables_secret);")
con.execute("SELECT * FROM s3_tables.silver.stg_orders LIMIT 10").fetchall()
```

**Via Athena:**
```bash
aws athena start-query-execution \
  --query-string "SELECT * FROM silver.stg_orders LIMIT 10" \
  --work-group aws-duckdb-lakehouse-dev \
  --query-execution-context Catalog=datalab-duckdb
```
Ou, no console do Athena, selecione `datalab-duckdb` no dropdown de "Data
source" antes de consultar.

---

## 6. Logs e execução no Fargate

### 6.1 Onde ver os logs

Não existe um bucket com os logs "principais" — o log corrente (stdout/
stderr do processo, o que aparece na tela durante o `dbt build`) vai pro
**CloudWatch Logs**:

- **Log group**: `/ecs/aws-duckdb-lakehouse/dev`
- **Stream**: `lakehouse/lakehouse/<task-id>`
- **Retenção**: 30 dias

```bash
# acompanhar ao vivo
aws logs tail /ecs/aws-duckdb-lakehouse/dev --follow

# um stream específico
aws logs get-log-events \
  --log-group-name "/ecs/aws-duckdb-lakehouse/dev" \
  --log-stream-name "lakehouse/lakehouse/<task-id>" \
  --query "events[].message" --output text
```

Além disso, o arquivo de log **interno** do próprio dbt (`logs/dbt.log`,
mais detalhado que o stdout) é enviado pro S3 depois de cada execução —
ver `aws_s3_bucket.dbt_logs` em `infra/s3.tf` e o upload em
`ingestion/entrypoint.py`:

- **Bucket**: `datalab-logs-dbt`
- **Chave**: `dbt/<data-UTC>/<mode>/<task-id>/dbt.log`
- **Retenção**: 90 dias (`dbt_logs_retention_days`)

```bash
aws s3 ls s3://datalab-logs-dbt/dbt/ --recursive
aws s3 cp s3://datalab-logs-dbt/dbt/2026-09-12/dbt-build/<task-id>/dbt.log -
```

O upload acontece num bloco `finally` em `entrypoint.py`, então roda mesmo
se o `dbt build` falhar (é exatamente quando o log detalhado costuma ser
mais útil) — e é best-effort: um erro no upload nunca mascara o código de
saída real do dbt.

### 6.2 Onde ver a task rodando/rodada no Fargate

**Console:** ECS → Clusters → `aws-duckdb-lakehouse-dev` → aba "Tasks"
(mostra tasks rodando e as paradas recentemente — clique numa task pra ver
CPU/memória, rede, e um link direto pro stream de log no CloudWatch).

**CLI:**
```bash
# tasks rodando agora
aws ecs list-tasks --cluster aws-duckdb-lakehouse-dev

# tasks paradas recentemente (histórico limitado — não é permanente)
aws ecs list-tasks --cluster aws-duckdb-lakehouse-dev --desired-status STOPPED

# detalhes de uma task específica
aws ecs describe-tasks --cluster aws-duckdb-lakehouse-dev --tasks <task-arn>
```

O histórico de tasks paradas no console/CLI do ECS **não é permanente** —
pra rastrear execuções antigas de forma confiável, use o CloudWatch Logs
(30 dias) ou o bucket de logs do dbt (90 dias), não a lista de tasks do
ECS.

Pra saber quando foi a próxima/última execução agendada:
```bash
aws scheduler get-schedule --name aws-duckdb-lakehouse-dev-dbt-build --group-name default
```

### 6.3 Cuidado ao mudar variáveis de ambiente da task definition

A `aws_ecs_task_definition.lakehouse` (em `infra/main.tf`) tem
`lifecycle { ignore_changes = [container_definitions] }` — isso existe de
propósito, pra o Terraform não brigar com o `deploy.yml` (que registra
novas revisões a cada deploy, trocando só a imagem). Só que isso tem uma
consequência importante: **rodar `terraform apply` depois de adicionar uma
env var nova ao `container_definitions` não propaga essa mudança pra task
definition real** — o Terraform ignora esse campo depois da criação
inicial.

Como o `deploy.yml` sempre parte da task definition **atualmente
registrada** (via `aws ecs describe-task-definition`) e só troca a imagem,
uma env var nova só passa a existir nos próximos deploys se você registrar
manualmente uma revisão nova que já a contenha — foi assim que
`DBT_LOG_BUCKET` foi adicionado (revisão 10):

```bash
aws ecs describe-task-definition --task-definition aws-duckdb-lakehouse-dev \
  --query "taskDefinition" --output json > taskdef.json

python3 -c "
import json
d = json.load(open('taskdef.json'))
for k in ['taskDefinitionArn','revision','status','requiresAttributes',
          'compatibilities','registeredAt','registeredBy']:
    d.pop(k, None)
d['containerDefinitions'][0]['environment'].append(
    {'name': 'MINHA_VAR_NOVA', 'value': 'valor'}
)
json.dump(d, open('taskdef.json', 'w'))
"

aws ecs register-task-definition --cli-input-json file://taskdef.json
rm taskdef.json
```

Depois disso, a nova revisão vira a "base" — os próximos `git push`
(que disparam o `deploy.yml`) vão carregar essa env var adiante
automaticamente, já que cada deploy só troca a imagem em cima do que já
está registrado.
