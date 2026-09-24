# Arquitetura — AWS - Datalab Project (dbt + DuckDB + S3 Tables + Athena)

Este documento descreve a arquitetura completa do projeto, o que foi
criado na AWS, os problemas encontrados ao subir a stack (e
como foi resolvido), como renomear os recursos principais, e como rodar o
pipeline local e na AWS. 

A escolha pelo Airflow no docker é devido aos custos envolvidos pra subir um MWAA ou um EC2 com RDS para rodar o Airflow self hosted, os demais recursos rodam na AWS para que a maior parte dos serviços seja na nuvem.

Ferramentas utilizadas:

 1. AWS
 2. Airflow (Rodando no Docker)

Acesso ao Airflow:

- Rodar o docker dompose up -d na pasta airflow
- Acessar a UI: http://localhost:8080
- Utilizar o usuário e senha: airflow / airflow

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
   dbt/models/silver/stg_{orders,clients,inventory}.sql
   (cast de tipos, normalização de texto, filtro de chaves nulas — e,
    a partir da 2a run, merge do lote CDC mais recente)

   dbt/models/silver/dim_client_portfolio.sql
   (dimensão SCD Tipo 2 da hierarquia de carteira — ver §8)
            |
            v
   DuckDB ATTACH ... (TYPE iceberg, ENDPOINT_TYPE s3_tables)
            |
            v
   S3 Tables — bucket dedicado, database `silver` (formato Iceberg)  <- camada silver
            |
            v
   dbt/models/gold/fct_portfolio_revenue.sql
   (agregado analítico com atribuição point-in-time contra a SCD2)
            |
            v
   S3 Tables — mesmo bucket, database `gold` (formato Iceberg)       <- camada gold
            |
            v
   Federação Glue Data Catalog (s3tablescatalog) + Lake Formation
            |
            v
   Athena (data source `datalab-duckdb`)
```

As entidades da camada silver têm integridade referencial de verdade, que é o que sustenta a camada
gold:

```text
orders.customer_id     -> clients.customer_id
orders.product_id      -> inventory.product_id
portfolios.customer_id -> clients.customer_id
orders.amount            é derivado de inventory.unit_cost * quantity (com ruído)
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

Comandos executados pra criar essa etapa: 

cd /Users/brunogdealmeida/Documents/Projetos/aws-dbt-duckdb/infra/bootstrap && terraform apply -auto-approve
  -var="state_bucket_name=aws-dbt-duckdb-tfstate-770724966330"

Se pedir um Value como parametro coloque: state_bucket_name=aws-dbt-duckdb-tfstate-770724966330

Essa etapa é necessária pois o Terraform precisa de um state permanente por isso ele precisa de um bucket pra armazenar e usar como memória persistente e o dynamodb fica responsável pelo state locking (acho que nas versões mais recentes o S3 faz o locking, mas eu ainda não me aprofundei nisso e vou deixar pra outra etapa)

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
| `s3tables.tf` | `aws_s3tables_table_bucket`, `aws_s3tables_namespace.silver`, `aws_s3tables_namespace.gold` | Camadas silver e gold (Iceberg), dois namespaces no mesmo table bucket |
| `glue_lakeformation.tf` | Role de federação, `aws_lakeformation_resource`, `aws_lakeformation_data_lake_settings`, `awscc_glue_catalog.s3tables`, `aws_lakeformation_permissions` | Torna o S3 Tables visível/consultável pelo Glue + Lake Formation |
| `athena.tf` | Bucket de resultados, `aws_athena_workgroup`, `aws_athena_data_catalog` (nome: `datalab-duckdb`) | Consulta via Athena |
| `scheduler.tf` | Role do EventBridge Scheduler, `aws_scheduler_schedule.dbt_build` | Roda `dbt build` todo dia às 03:00 UTC |
| `outputs.tf` | Nomes/ARNs de tudo acima | Referência rápida (`terraform output`) |

### 2.3 dbt (`dbt/`)

| Arquivo | Propósito |
|---|---|
| `profiles.yml` | Target `dev` (100% local, sem AWS) e `prod` (ECS, anexa o S3 Tables via `attach`/`secrets`) |
| `dbt_project.yml` | Config condicional por target: materialização, `database`, `schema` |
| `models/sources.yml` | Fontes bronze (`orders`, `clients`, `inventory` + `*_cdc`, `portfolios`), lidas via `external_location` |
| `models/silver/schema.yml` | Testes básicos (`unique`, `not_null`, `accepted_values`, `relationships`) — só têm efeito com `dbt build`/`dbt test`, não com `dbt run` (ver §3.20) |
| `models/silver/stg_{orders,clients,inventory}.sql` | Incrementais: baseline (cast, normalização, filtro de PK) direto de `bronze.<entidade>` na primeira run, depois aplicam o lote CDC mais recente de `bronze.<entidade>_cdc` (`incremental_strategy='cdc_merge'`) — ver §6 |
| `macros/generate_schema_name.sql` | Faz o schema resolver pra `silver` (não `main_silver`, que é o padrão do dbt) |
| `macros/materialization_iceberg_table.sql` | Materialização customizada pro target `prod` (ver §3.7) |
| `models/silver/dim_client_portfolio.sql` | Dimensão **SCD Tipo 2** da hierarquia de carteira de clientes (`incremental_strategy='scd2_merge'`) — ver §8 |
| `models/gold/fct_portfolio_revenue.sql` | Camada **gold**: receita mensal por carteira/gerente/região, com atribuição point-in-time contra a SCD2 — ver §8 |
| `models/gold/schema.yml` | Testes da camada gold |
| `tests/assert_scd2_*.sql` | Testes singulares que provam as invariantes da SCD2 (sem sobreposição de versões; `is_current` coerente com `valid_to`) — ver §3.23 |
| `macros/incremental_strategy_cdc_merge.sql` | Estratégia incremental customizada `cdc_merge` (DELETE + MERGE em dois statements — ver §3.17) |
| `macros/incremental_strategy_scd2_merge.sql` | Estratégia incremental customizada `scd2_merge`, que mantém a SCD Tipo 2 (UPDATE + INSERT em dois statements — ver §7.4) |

### 2.4 Ingestão (`ingestion/`)

| Arquivo | Propósito |
|---|---|
| `ingest_csv.py` | Sobe CSVs locais pro landing bucket (`bronze/<source>/`) |
| `generate_seed_data.py` | Gera dados de teste em volume real via DuckDB (orders=5M, clients=100k, inventory=1M linhas, + snapshot de hierarquia de carteira: 500 carteiras / 50 gerentes), com FKs íntegras |
| `simulate_cdc.py` | Gera e sobe lotes de CDC (insert/update/delete) pra simular mudanças incrementais — ver §6 |
| `simulate_portfolio_changes.py` | Reescreve o snapshot da hierarquia de carteira (clientes trocam de carteira; carteiras trocam de gerente/região) pra alimentar a SCD Tipo 2 — ver §8 |
| `sample_data/*.csv` | Amostras pequenas e FK-consistentes, usadas pelo target `dev`/CI |
| `sample_data/*_cdc.csv` | Lotes de CDC de exemplo (mão), usados pelo target `dev`/CI pros models incrementais |

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

### 3.16 dbt não encontra a estratégia incremental customizada `cdc_merge`

**Sintoma:** `Compilation Error: dbt could not find an incremental
strategy macro with the name 'get_incremental_cdc_merge_sql' in
aws_lakehouse` — mesmo com a macro `duckdb__get_incremental_cdc_merge_sql`
já implementada e nomeada certo.
**Causa:** `adapter.get_incremental_strategy_macro()` resolve o nome
**sem prefixo** (`get_incremental_cdc_merge_sql`) via
`adapter.dispatch(...)`, do mesmo jeito que as estratégias nativas do dbt
(`get_incremental_merge_sql`, etc.) são declaradas — só ter a versão
prefixada com `duckdb__` não é suficiente, o dispatcher plain precisa
existir.
**Correção:** adicionar a macro pública (sem prefixo) que só chama
`adapter.dispatch('get_incremental_cdc_merge_sql')(args_dict)`, igual ao
padrão usado pelas estratégias built-in do dbt-core (ver
`dbt/macros/incremental_strategy_cdc_merge.sql`).

### 3.17 `MERGE INTO` do DuckDB em tabela Iceberg só aceita uma ação (UPDATE ou DELETE)

**Sintoma:** `Not implemented Error: MERGE INTO with Iceberg only supports
a single UPDATE/DELETE action currently` — ao rodar a estratégia `merge`
nativa do dbt-duckdb (que gera um único `MERGE INTO` com `WHEN MATCHED AND
... THEN DELETE` e `WHEN MATCHED THEN UPDATE` no mesmo statement).
**Causa:** limitação real da extensão Iceberg do DuckDB 1.5.5 — descoberta
testando direto contra o bucket S3 Tables real, não documentada
previamente. Um `MERGE INTO` visando uma tabela Iceberg aceita **no
máximo uma** ação de UPDATE/DELETE.
**Correção:** estratégia customizada `cdc_merge` em dois statements —
(1) `DELETE FROM target WHERE pk IN (SELECT pk FROM lote WHERE
_cdc_op='D')`, depois (2) um `MERGE INTO` só com `WHEN MATCHED THEN UPDATE
SET *` / `WHEN NOT MATCHED THEN INSERT *` (sem cláusula de delete).
Confirmado que os dois statements, separados por `;`, rodam certinho numa
única chamada `execute()` — que é como o dbt-duckdb executa o SQL
compilado de uma materialização.

### 3.18 `simulate_cdc.py` gerava linhas marcadas como 'U' **e** 'D' ao mesmo tempo

**Sintoma:** depois de aplicar um lote de CDC em escala real (5M+ linhas),
a contagem final ficou **maior** que o esperado (`5.000.303` em vez de
`4.999.759` em `orders`, e o mesmo padrão em `clients`/`inventory`) —
descoberto comparando a aritmética esperada (baseline − deletes +
inserts) com o resultado real via Athena, não assumido como correto só
porque o `dbt build` reportou sucesso.
**Causa:** o script sorteava as linhas de update e de delete com duas
chamadas `USING SAMPLE X% (bernoulli)` **independentes** sobre a mesma
tabela base — como cada sorteio é independente, uma linha podia cair nos
dois grupos ao mesmo tempo (mesma PK marcada `'U'` e `'D'` no mesmo lote).
A estratégia `cdc_merge` (§3.17) processa o `DELETE` primeiro e o
`MERGE`/`INSERT` depois: a cópia `'D'` deletava a linha, e a cópia `'U'`
da mesma PK, não encontrando mais correspondência, caía no `WHEN NOT
MATCHED THEN INSERT` — reinserindo (com os valores "atualizados") uma
linha que deveria ter sido removida. Bug do script de simulação, não da
estratégia de merge, dos models, nem do `MERGE`/`DELETE` do DuckDB
(validados isoladamente com dados sem sobreposição antes disso).
**Correção:** sortear **um único** valor aleatório por linha (`random()
AS _r` numa CTE) e particionar update/delete a partir desse mesmo valor
(`_r < delete_pct` vs. `delete_pct <= _r < delete_pct+update_pct`), em vez
de duas amostragens independentes — garante que update e delete são
mutuamente exclusivos dentro do mesmo lote.

### 3.19 Reaplicar vários lotes de CDC acumulados dá resultado ambíguo

**Sintoma (encontrado testando, antes de virar bug em produção):** a fonte
`bronze/<entidade>/cdc/*.csv` é um `glob` achatado — cada `dbt build` lê
**todo** arquivo já subido naquele prefixo, não só os novos. Se a mesma PK
aparece em dois lotes históricos diferentes (ex.: atualizada de novo numa
rodada seguinte), o `MERGE INTO` do DuckDB recebe duas linhas de origem
pra uma mesma linha de destino.
**Causa:** confirmado com um `MERGE INTO` isolado (fora do dbt) com duas
linhas de origem pra mesma chave — DuckDB **não** rejeita nem avisa,
resolve o conflito escolhendo uma das duas silenciosamente (a primeira, no
teste; não necessariamente a mais recente), o que corrompe o resultado de
forma sutil ao simular várias rodadas de mudança.
**Correção:** `simulate_cdc.py` agora arquiva (`copy_object` + `delete_object`)
os lotes já subidos pra `bronze/<entidade>/cdc/applied/` antes de escrever
o próximo — um nível a mais no caminho, então não batem mais no glob raso
`cdc/*.csv`. Assim o glob nunca vê mais de um lote de cada vez. Desabilite
com `--keep-previous-batches` só se você quiser deliberadamente acumular
lotes (não recomendado).

### 3.20 `sources.yml`/`schema.yml` sem testes — teste `relationships` achou órfão nos dados de amostra

**Contexto:** ao adicionar `dbt/models/silver/schema.yml` com testes
básicos (`unique`, `not_null`, `accepted_values`, `relationships`), rodar
`dbt build --target dev` **duas vezes seguidas** (o target `dev` usa um
arquivo DuckDB persistente em `/tmp/dbt_dev.duckdb`, então a segunda run
já é incremental e aplica `ingestion/sample_data/*_cdc.csv`) falhou nos
testes `relationships` de `orders`.
**Causa:** os CSVs de exemplo do CDC não preservavam integridade
referencial entre si — `clients_cdc.csv` deleta o cliente `104` e
`inventory_cdc.csv` deleta o produto `4`, mas `orders_cdc.csv` não tocava
na order `4` (que referencia os dois), deixando-a órfã depois do merge.
Não era um bug da estratégia de merge nem do dbt — os testes fizeram
exatamente o que deveriam, achando uma inconsistência real nos dados de
demonstração que antes passava despercebida por falta de teste.
**Correção:** adicionada a linha `4,104,4,3,2026-01-06,44.25,cancelled,D`
em `orders_cdc.csv`, deletando a order junto com o cliente/produto.
Validado rodando `dbt build --target dev` três vezes seguidas (baseline,
incremental, e reaplicação do mesmo lote) — 32/32 testes passando nos três
casos.

### 3.21 Consolidação de `stg_*.sql` + `{orders,clients,inventory}.sql` num só model (três bugs)

**Contexto:** os três models full-refresh (`stg_orders`/`stg_clients`/
`stg_inventory`) e os três incrementais (`orders`/`clients`/`inventory`)
foram consolidados num único arquivo por entidade — `stg_*.sql` passou a
ser, ele mesmo, o model incremental (baseline na primeira run, CDC depois),
e os três arquivos separados foram apagados. A mudança expôs três bugs,
achados rodando `dbt compile`/`dbt build --target dev` de verdade (não só
lendo o SQL):

1. **Ciclo de dependência.** A branch `{% else %}` (baseline) de cada
   model referenciava **a si mesma** (`from {{ ref('stg_orders') }}`
   dentro do próprio `stg_orders.sql`) — sobrou do formato antigo, em que
   o model incremental lia o `stg_*` separado. `dbt compile` falha com
   `Found a cycle: model.aws_lakehouse.stg_orders`. **Correção:** a branch
   baseline agora lê direto de `{{ source('bronze', '<entidade>') }}`
   (a fonte raw, sem CDC), com o mesmo cast/normalização que antes vivia
   no `stg_*` separado.
2. **`current_timestamp()` não existe no DuckDB.** As duas colunas de
   metadata adicionadas (`ingestion_time`, `last_updated_time`) usavam
   `current_timestamp()` com parênteses — DuckDB trata `current_timestamp`
   como palavra-chave niládica, não função escalar:
   `Catalog Error: Scalar Function with name current_timestamp does not
   exist! Did you mean "current_localtimestamp"?`. **Correção:** removidos
   os parênteses (`current_timestamp`, sem `()`) nas seis ocorrências.
3. **Contagem de colunas diferente entre as duas branches quebra o
   `INSERT *`/`UPDATE SET *` do `cdc_merge`.** A branch baseline ganhou
   uma coluna extra (`ingestion_time`) que a branch incremental não tinha
   — o `WHEN NOT MATCHED THEN INSERT *` do merge (§3.17) exige que a
   sub-query de origem tenha o mesmo número de colunas da tabela alvo:
   `Binder Error: table stg_inventory has 9 columns but 8 values were
   supplied`. **Correção:** `ingestion_time` adicionada também na branch
   incremental (com `current_timestamp`), igualando as duas a 9 colunas.
   **Limitação adicional, corrigida depois (ver §3.22):** como o merge
   fazia `UPDATE SET *` (todas as colunas da origem, sem exceção), uma
   linha que sofria um `U` do CDC tinha `ingestion_time` sobrescrito para a
   hora atual, perdendo o registro de quando a linha foi carregada pela
   primeira vez.

### 3.22 Preservar `ingestion_time` (auditoria de carga) através de updates do CDC

**Objetivo:** `ingestion_time` deve registrar quando a linha entrou na
tabela pela primeira vez, pra auditoria — diferente de `last_updated_time`,
que deve refletir a última mudança. O `UPDATE SET *` do `cdc_merge`
(§3.21) sobrescrevia as duas colunas igualmente em todo update,
inutilizando essa distinção.
**Correção:** o macro `duckdb__get_incremental_cdc_merge_sql` (ver §7)
agora monta a lista de colunas do `UPDATE SET` explicitamente a partir de
`args_dict['dest_columns']` (já fornecido pelo dbt-core), **excluindo**
qualquer coluna listada na config do model
`cdc_merge_preserve_on_update` — essas ficam de fora do `SET`, então o
`MERGE INTO` mantém o valor já existente em `DBT_INTERNAL_DEST` em vez de
trazer o da origem. `INSERT *` continua trazendo todas as colunas (linha
nova, `ingestion_time` = agora, correto). Os três models
(`stg_orders`/`stg_clients`/`stg_inventory`) declaram
`cdc_merge_preserve_on_update=['ingestion_time']`.
**Validado:** rodando baseline → incremental (update em order 1, insert de
order 6) → reaplicação. `ingestion_time` do order 1 permaneceu no valor da
carga original através das três rodadas; `last_updated_time` avançou a
cada vez que a linha foi tocada; linhas nunca tocadas mantiveram os dois
campos iguais desde a carga inicial.

Revalidado de ponta a ponta (`dbt parse`, `dbt compile`, e `dbt build
--target dev` três vezes seguidas — baseline, incremental, reaplicação)
depois das três correções, sem erros nem warnings.

### 3.23 Teste de SCD2 dava falso positivo em cliente que sai e volta

**Sintoma:** ao validar a dimensão SCD Tipo 2 (§8), o teste singular
`assert_scd2_timeline_contiguous` falhou num cenário perfeitamente
legítimo — um cliente que saiu do snapshot da hierarquia numa carga e
voltou na seguinte.
**Causa:** o teste, como escrito primeiro, exigia que o `valid_to` de uma
versão fosse **exatamente** o `valid_from` da seguinte, tratando qualquer
buraco na linha do tempo como defeito. Mas quando a chave some do snapshot
a estratégia fecha a versão vigente e **não** abre nenhuma nova (correto:
aquele cliente deixou de ter carteira); se ele reaparece depois, uma nova
versão é aberta na data do retorno. O intervalo entre as duas não é um bug
— é justamente o período em que ele não tinha carteira, e o join
point-in-time do gold tem que não casar nada ali mesmo.
**Correção:** o teste passou a checar só o que de fato corrompe o
resultado — **sobreposição** (`valid_to > valid_from` da seguinte, que faria
um fato casar com duas versões e ser contado em dobro) e versão não fechada
com sucessora. Buracos deixaram de ser erro, com o porquê registrado no
próprio arquivo de teste.
**Verificado que o teste ainda falha quando deve:** injetando uma linha com
intervalo sobreposto na dimensão, ele acusou (`Got 1 result, configured to
fail if != 0`) — sem essa checagem, a correção acima poderia ter virado um
teste que nunca pega nada.

### 3.24 `simulate_portfolio_changes.py`: dois jeitos de corromper o snapshot em silêncio

Os dois apareceram testando o script de simulação da hierarquia, não em
produção — e os dois seriam invisíveis até estragarem a SCD2 lá na frente.

**(a) Carteira com atributos divergentes abria o join em leque.** O catálogo
de carteiras saía de um `SELECT DISTINCT portfolio_id, portfolio_name,
manager_id, ...` do snapshot. Se a mesma carteira aparecesse com atributos
diferentes (drift da origem, ou snapshot montado à mão), o DISTINCT devolvia
várias linhas pro mesmo `portfolio_id` e o join com a atribuição de clientes
multiplicava linhas — 1.000 clientes viraram 2.943. Na SCD2 isso abriria
**duas versões vigentes pro mesmo cliente**, quebrando todo join
point-in-time daí pra frente. **Correção:** `QUALIFY row_number() OVER
(PARTITION BY portfolio_id ...) = 1`, garantindo uma linha por carteira
independente da qualidade da entrada.

**(b) Sorteio de carteira assumia ids densos e descartava clientes.** Uma
reatribuição sorteava `1 + random()*(n-1)` como se fosse o próprio
`portfolio_id`, o que só vale enquanto os ids forem 1..n contíguos. Não são:
uma carteira que perde todos os clientes some do snapshot. A partir daí o
sorteio apontava pra id inexistente e o cliente era **descartado no join** —
2.000 clientes viraram 1.998 na terceira rodada. O efeito na SCD2 seria
ainda pior que perder linhas: clientes sumindo do snapshot são interpretados
como saída legítima da hierarquia, então suas versões seriam *fechadas*, do
jeito certo, por um motivo errado. **Correção:** sortear pelo índice de um
`row_number()` sobre as carteiras que de fato existem, nunca pelo id.

**O que achou os dois:** um guard de invariante no fim do `mutate()` — o
snapshot de saída tem que ter exatamente os mesmos clientes da entrada, já
que o script só muda atributos e nunca a população. O (a) foi pego por um
teste com snapshot propositalmente malformado; o (b) só apareceu na 3ª
rodada seguida, e por isso o script passou a ser testado com 8 rodadas
encadeadas, em que dá pra ver as carteiras encolhendo (490 -> 482) sem a
população de clientes mudar.

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
`orders` toda vez): `dbt build --target prod --select clients inventory`.

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

**Opção D — a partir do Airflow:** `airflow/dags/dbt_lakehouse_dag.py`
dispara essa mesma task ECS via `EcsRunTaskOperator` — mesma imagem, mesma
task definition, sem precisar instalar dbt/DuckDB dentro do Airflow. Três
jeitos de rodar (detalhes em `airflow/README.md`): sua própria instância; um
Airflow completo em Docker já configurado em `airflow/`
(`docker compose up -d`, **recomendado no macOS** — validado de ponta a
ponta); ou uma instância local em `venv` sem Docker (`./start.sh`/
`./stop.sh`) — essa última esbarra num bug real do `LocalExecutor` do
Airflow no macOS (trava resolvendo DNS ao despachar a task de verdade,
mesmo sem estar em Docker) documentado em `airflow/README.md`.

### 5.4 Consultando o resultado

**Via DuckDB direto** (funciona igual ao que o `prod` target faz):
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; INSTALL aws; INSTALL iceberg; LOAD httpfs; LOAD aws; LOAD iceberg;")
con.execute("CREATE SECRET s3_tables_secret (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
con.execute("ATTACH IF NOT EXISTS 'arn:aws:s3tables:us-east-1:770724966330:bucket/dbt-duckdb-tables-770724966330' "
            "AS s3_tables (TYPE iceberg, ENDPOINT_TYPE s3_tables, SECRET s3_tables_secret);")
con.execute("SELECT * FROM s3_tables.silver.orders LIMIT 10").fetchall()
```

**Via Athena:**
```bash
aws athena start-query-execution \
  --query-string "SELECT * FROM silver.orders LIMIT 10" \
  --work-group aws-duckdb-lakehouse-dev \
  --query-execution-context Catalog=datalab-duckdb
```
Ou, no console do Athena, selecione `datalab-duckdb` no dropdown de "Data
source" antes de consultar.

---

## 6. Simulando CDC (updates/deletes incrementais)

Os models `dbt/models/silver/stg_{orders,clients,inventory}.sql` são
**incrementais de verdade**: a primeira run materializa a tabela inteira
direto de `bronze.<entidade>` (cast, normalização, filtro de PK); toda run
seguinte lê o(s) lote(s) de CDC mais recente(s) de `bronze.<entidade>_cdc`
— linhas marcadas com uma coluna `_cdc_op` (`I` insert/`U` update/`D`
delete) — e aplica só a mudança, via a estratégia customizada `cdc_merge`
(§3.17/§3.16).

**Gerando um lote de CDC** (requer os full-loads já gerados por
`generate_seed_data.py`):
```bash
python ingestion/simulate_cdc.py --entity all
# ou, uma entidade por vez, com percentuais customizados:
python ingestion/simulate_cdc.py --entity orders --update-pct 5 --delete-pct 1 --insert-pct 1
```
Escreve `ingestion/seed_data/bronze/<entidade>/cdc/<timestamp>.csv` e sobe
pro `LANDING_BUCKET` (env var) em `bronze/<entidade>/cdc/`. `--no-upload`
só grava local. O estado (próximo ID livre de cada entidade, pra novos
inserts não colidirem com PKs existentes) fica em
`ingestion/seed_data/.cdc_state.json` — não commitado, cresce a cada run.

**Aplicando o lote:**
```bash
dbt build --target prod --select orders clients inventory
```
Roda a materialização incremental, que faz `glob` em
`bronze/<entidade>/cdc/*.csv`. Esse glob é achatado (só um nível) — por
isso o `simulate_cdc.py` arquiva os lotes já subidos em
`cdc/applied/` antes de subir o próximo (ver §3.19): assim o glob sempre
enxerga só o lote mais recente na hora do `dbt build`, e basta rodar
`simulate_cdc.py` de novo pra simular uma nova rodada de mudanças.

**Validado em escala real** contra o S3 Tables de produção: baseline
(5M+100k+1M linhas) em ~21s, incremental (~125 mil mudanças contra a
tabela de 5M linhas) em ~36s.

---

## 7. Macros (`dbt/macros/`)

Três macros customizadas, cada uma resolvendo uma limitação específica do
dbt-duckdb ou do DuckDB contra S3 Tables/Iceberg (todas com o bug/causa
documentado em detalhe na seção 3).

### 7.1 `generate_schema_name.sql` — nome do schema sem prefixo

**O que é:** um *override* de uma macro padrão do dbt-core
(`generate_schema_name`), que o dbt chama **automaticamente** sempre que
resolve o schema de qualquer model — não precisa ser referenciada em
lugar nenhum, o dbt encontra pelo nome exato.
**Problema que resolve:** por padrão, quando um model define
`+schema: "silver"` (como os models de `silver/` fazem, via
`dbt_project.yml`), o dbt-core **concatena** com o schema do target
(`{{ target.schema }}_silver`, ex.: `main_silver`) em vez de usar
`silver` puro. O namespace real criado no S3 Tables pelo Terraform
(`infra/s3tables.tf`) se chama `silver`, sem prefixo — ver §3.
**Como funciona:** recebe `custom_schema_name` (o valor de `+schema`
configurado no model) e `node` (o model sendo compilado). Se não há
schema customizado, cai no padrão do target; se há, devolve **só** o
nome customizado (`custom_schema_name | trim`), ignorando o prefixo que
o dbt normalmente adicionaria.

### 7.2 `materialization_iceberg_table.sql` — materialização `iceberg_table`

**O que é:** uma materialização customizada nova (não um override) —
fica disponível como `{{ config(materialized='iceberg_table') }}` em
qualquer model, e é o default configurado em `dbt_project.yml` para a
pasta `silver/` no target `prod` (`+materialized: "{{ 'iceberg_table' if
target.name == 'prod' else 'table' }}"`).
**Status atual:** nenhum model usa `iceberg_table` hoje — os três models
de `silver/` (`stg_orders`/`stg_clients`/`stg_inventory`) fixam
`materialized='incremental'` direto na própria config, que tem prioridade
sobre o default da pasta. A materialização fica como *fallback* pronta
pra qualquer model full-refresh que venha a ser adicionado em `silver/`
(ou numa futura camada `gold/`) contra o target `prod`.
**Problema que resolve (quando usada):** a materialização `table` padrão
do dbt-duckdb cria uma tabela temporária e troca o nome por um `RENAME`
atômico — o catálogo Iceberg do DuckDB não suporta `RENAME`
(`Not implemented Error: Alter Schema Entry`, confirmado contra o bucket
real).
**Como funciona:** se já existe uma relação com esse nome, roda um
`DROP TABLE IF EXISTS` direto (não usa `adapter.drop_relation()`, que
emite `DROP ... CASCADE`, também não suportado no Iceberg) e dá um
`adapter.commit()` explícito **antes** do `CREATE` — sem isso, a extensão
Iceberg rejeita criar uma tabela com nome igual a uma apagada na mesma
transação ainda aberta (`Cannot create table deleted within a
transaction`). Depois roda um `CREATE TABLE ... AS` normal
(`create_table_as`). Ou seja: drop-and-recreate completo a cada run, em
vez do swap atômico que a materialização padrão faria.

### 7.3 `incremental_strategy_cdc_merge.sql` — estratégia `cdc_merge`

**O que é:** duas macros que juntas implementam uma *incremental
strategy* customizada, usada via `{{ config(materialized='incremental',
incremental_strategy='cdc_merge', unique_key=...) }}` nos três models de
`silver/`.
**Por que duas macros:** a materialização `incremental` do dbt-core
resolve o nome da estratégia (`cdc_merge` → `get_incremental_cdc_merge_sql`)
via `adapter.dispatch(...)`, e esse dispatch só encontra a macro se
existir uma versão **sem prefixo** de adapter que ela mesma chame
`adapter.dispatch(...)` de novo — é assim que as estratégias nativas do
próprio dbt-core (`get_incremental_merge_sql`, etc.) são declaradas. Por
isso:
- `get_incremental_cdc_merge_sql(args_dict)` — só repassa pra
  `adapter.dispatch('get_incremental_cdc_merge_sql')(args_dict)`. Sem
  essa camada, o dbt erra com "could not find an incremental strategy
  macro" mesmo com a implementação abaixo já existindo (ver §3.16).
- `duckdb__get_incremental_cdc_merge_sql(args_dict)` — a implementação de
  verdade, específica do adapter `duckdb` (prefixo `duckdb__` é a
  convenção do dbt pra "isso vale só nesse adapter").

**Como a implementação funciona**, passo a passo:
1. Extrai do `args_dict` (montado pelo dbt-core): a relação alvo
   (`target_relation`, ex. `silver.orders`), a relação temporária com o
   lote compilado do model (`temp_relation`), a chave única
   (`unique_key`) e as colunas da tabela alvo (`dest_columns`, já
   calculadas pelo dbt-core via `adapter.get_columns_in_relation`).
2. Lê a config opcional do model `cdc_merge_preserve_on_update` (lista de
   nomes de coluna, default `[]`) e monta `update_columns` — todas as
   colunas de `dest_columns` **exceto** essas.
3. Statement 1 — `DELETE`: apaga do alvo toda linha cuja chave apareça na
   origem com `_cdc_op = 'D'`.
4. Statement 2 — `MERGE INTO`: casa o alvo com a origem (excluindo as
   linhas `'D'`, já tratadas, e a própria coluna `_cdc_op`) pela
   `unique_key`. Quando casa (`WHEN MATCHED`), roda um `UPDATE SET`
   **explícito**, coluna por coluna, só com as de `update_columns` — as
   listadas em `cdc_merge_preserve_on_update` (ex.: `ingestion_time`)
   ficam de fora do `SET`, então o `MERGE` mantém o valor que já estava
   no alvo em vez de trazer o da origem (ver §3.22). Quando não casa
   (`WHEN NOT MATCHED`), roda `INSERT *` — traz todas as colunas da
   origem, `ingestion_time` incluído (linha nova, valor correto).
   Dois motivos pra ser dois statements e não um `MERGE` só com
   `DELETE`+`UPDATE`: (a) o Iceberg do DuckDB só aceita uma ação de
   UPDATE/DELETE por `MERGE` (§3.17); (b) mesmo sem essa limitação, duas
   linhas de origem pra mesma chave dentro do mesmo lote de CDC dariam
   resultado ambíguo (§3.19/§3.20) — o `DELETE` isolado evita essa
   ambiguidade pro caso de delete. Os dois statements, separados por
   `;`, rodam numa única chamada `execute()` do DuckDB — é assim que o
   dbt-duckdb executa o SQL compilado de uma materialização.

### 7.4 `incremental_strategy_scd2_merge.sql` — estratégia `scd2_merge`

**O que é:** o par de macros (repassador sem prefixo +
implementação `duckdb__`, pelo mesmo motivo de dispatch explicado em §7.3)
que mantém uma dimensão **SCD Tipo 2** em cima de uma tabela Iceberg. Usada
por `models/silver/dim_client_portfolio.sql` via
`incremental_strategy='scd2_merge'`.

**O que recebe:** o model entrega um *snapshot completo* do estado atual —
uma linha por chave natural, já com `scd_hash` (hash só dos atributos
rastreados), `valid_from`, `valid_to` e `is_current`. A macro não sabe nada
do negócio; ela só compara esse snapshot com o que já está na dimensão.

**Como funciona**, em dois statements:

1. **UPDATE — fecha versões.** Marca `valid_to` = timestamp do lote e
   `is_current` = false em toda versão vigente cuja chave (a) sumiu do
   snapshot ou (b) teve o `scd_hash` alterado. O timestamp do lote sai de
   `max(valid_from)` do *próprio snapshot*, não de um `current_timestamp`
   novo — assim o `valid_to` da versão fechada é exatamente o `valid_from`
   da que vai abrir, sem buraco nem sobreposição na linha do tempo.
2. **INSERT — abre versões.** Depois do passo 1, toda chave que mudou ficou
   sem versão vigente, então um único anti-join (`left join ... where t.<pk>
   is null`) pega de uma vez as chaves novas **e** as que acabaram de ser
   fechadas — e ignora as inalteradas, que seguem com a versão aberta e não
   geram histórico à toa.

**Por que dois statements e não um MERGE:** uma chave que mudou precisa de
UPDATE (fechar a antiga) *e* INSERT (abrir a nova) para a mesma linha de
origem, o que um MERGE não expressa; e o Iceberg do DuckDB ainda por cima
só aceita uma ação de UPDATE/DELETE por MERGE (§3.17). Confirmado direto
contra o bucket real do S3 Tables, antes de escrever a macro, que as duas
formas usadas aqui funcionam no Iceberg: `UPDATE ... WHERE <chave> IN
(subquery)` e `INSERT ... SELECT` com anti-join contra a própria tabela
alvo.

**Configs opcionais do model**, caso as colunas de controle tenham outros
nomes: `scd2_hash_column` (default `scd_hash`), `scd2_valid_from_column`
(`valid_from`), `scd2_valid_to_column` (`valid_to`) e
`scd2_is_current_column` (`is_current`).

---

## 8. SCD Tipo 2: hierarquia de carteira de clientes + camada gold

### 8.1 O problema que a SCD2 resolve

A hierarquia de carteira (**cliente -> carteira -> gerente -> região/tier**)
muda com o tempo: cliente migra de carteira, carteira troca de gerente.
Se a dimensão guardasse só o estado atual, todo relatório histórico seria
reescrito a cada mudança — a receita que o gerente A trouxe no ano passado
apareceria como do gerente B só porque a carteira mudou de dono ontem.
Comissionamento, meta e série histórica ficam errados.

A dimensão `silver.dim_client_portfolio` guarda **uma linha por versão**:

| coluna | papel |
|---|---|
| `portfolio_version_key` | chave substituta da versão (PK da dimensão) |
| `customer_id` | chave natural (várias linhas por cliente, uma por versão) |
| `portfolio_id`, `manager_id`, `region`, `tier` | atributos rastreados |
| `portfolio_name`, `manager_name` | descritivos, **fora** do hash de propósito (um rename não gera versão nova) |
| `scd_hash` | hash dos atributos rastreados — é ele que decide se mudou |
| `valid_from` / `valid_to` | intervalo de validade, fechado-aberto `[valid_from, valid_to)` |
| `is_current` | `true` na versão vigente (redundante com `valid_to is null`, mas é o filtro barato do dia a dia) |

A carga inicial abre a primeira versão com `valid_from = 1900-01-01`, não
com o instante do primeiro `dbt build`: sem isso, nenhum pedido histórico
casaria com nenhuma versão e a camada gold sairia vazia.

### 8.2 A análise no gold

`gold.fct_portfolio_revenue` agrega receita mensal por
carteira/gerente/região fazendo o **join point-in-time** — cada pedido casa
com a versão vigente *na data do pedido*:

```sql
join dim_client_portfolio d
  on d.customer_id = o.customer_id
 and cast(o.order_date as timestamp) >= d.valid_from
 and (d.valid_to is null or cast(o.order_date as timestamp) < d.valid_to)
```

O intervalo é fechado-aberto pra que um pedido feito exatamente no instante
da troca caia só na versão nova. `valid_from`/`valid_to` são `timestamp`
sem timezone de propósito: comparar com `order_date` (DATE) em
`timestamptz` deixaria o resultado dependente do fuso da sessão.

Grão: uma linha por (carteira-na-época × mês). Métricas separam pago de
pendente/estornado (`paid_revenue`, `pending_amount`, `refunded_amount`) em
vez de somar tudo junto.

### 8.3 Como rodar e simular mudanças

```bash
python ingestion/generate_seed_data.py          # inclui o snapshot inicial da hierarquia
dbt build --target prod --select dim_client_portfolio+   # carga inicial + gold

# simula uma rodada de mudanças e reprocessa
python ingestion/simulate_portfolio_changes.py --reassign-pct 3 --rehome-pct 5
dbt build --target prod --select dim_client_portfolio+
```

`simulate_portfolio_changes.py` **substitui** o snapshot
(`bronze/portfolios/portfolios.csv`, mesma key no S3) em vez de acumular
arquivos como o `simulate_cdc.py` faz — a fonte aqui é snapshot completo, e
dois arquivos no mesmo prefixo fariam o glob devolver o cliente duas vezes
e abrir duas versões vigentes pra ele.

### 8.4 O que foi validado

Rodado de ponta a ponta no target `dev`, cobrindo os quatro caminhos da
estratégia — com o resultado conferido linha a linha, não só "o dbt disse
que passou":

| cenário | esperado | resultado |
|---|---|---|
| carga inicial | 1 versão por cliente, `valid_from` = 1900-01-01 | ✅ 6 clientes, 6 versões vigentes |
| cliente troca de carteira | fecha a antiga + abre a nova | ✅ 2 linhas, `valid_to` da antiga = `valid_from` da nova |
| carteira troca de gerente | versiona **todos** os clientes dela | ✅ 105 e 106 versionados juntos |
| cliente sem mudança | nada acontece | ✅ segue com 1 linha, sem versão nova |
| rodar de novo sem mudança | idempotente | ✅ contagem não muda |
| cliente some do snapshot | fecha a vigente, **não** abre nova | ✅ |
| cliente reaparece | abre versão nova (com buraco legítimo — §3.23) | ✅ |

E a prova de que a atribuição point-in-time funciona: depois de mover o
cliente 101 da carteira 1 (gerente 1/BR) para a 2 (gerente 2/US), os
pedidos dele de janeiro **continuaram** atribuídos à carteira 1 no gold —
o histórico não foi reescrito.

38/38 testes passando (incluindo os dois singulares da SCD2) em três
`dbt build` seguidos.

Detalhe do deploy: a task definition do ECS tem
`lifecycle { ignore_changes = [container_definitions] }` e o `deploy.yml`
registra revisões novas fora do Terraform (§3.15), então a variável
`S3_TABLES_GOLD_NAMESPACE` adicionada em `infra/main.tf` **não** chega
sozinha ao container. Não é problema: o `dbt_project.yml` usa
`env_var('S3_TABLES_GOLD_NAMESPACE', 'gold')`, e o default `gold` é
exatamente o valor do Terraform — a variável está lá por clareza, não por
necessidade.

**Ainda não validado contra a AWS real:** as mecânicas de SQL que a
estratégia usa foram testadas direto no bucket Iceberg de produção (§7.4),
mas o namespace `gold` é infra nova — depende de um `terraform apply` que
ainda não foi feito. O `terraform validate`/`plan` também não rodou aqui
(sem acesso de rede ao registry de providers); só `terraform fmt -check`,
que passou.

