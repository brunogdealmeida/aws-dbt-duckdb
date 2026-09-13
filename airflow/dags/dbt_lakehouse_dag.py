# DAG that triggers the SAME ECS Fargate task this project already uses for
# EventBridge Scheduler / manual GitHub Actions dispatch — see
# ARCHITECTURE.md §7. Airflow doesn't run dbt itself: it just calls
# ecs:RunTask with MODE=dbt-build and waits, reusing the exact Docker image
# (with dbt-core/dbt-duckdb/duckdb already installed and tested) that CI/CD
# builds. This avoids duplicating that whole toolchain into the Airflow
# environment and avoids handling S3 Tables credentials in a second place.
#
# Requirements in your Airflow environment:
#   pip install apache-airflow-providers-amazon
#
# AWS credentials for the Airflow worker (pick one):
#   1. Mount ~/.aws (read-only) into the Airflow containers and leave
#      AWS_CONN_ID = None below (boto3's default credential chain).
#   2. Configure an Airflow Connection (Admin -> Connections) of type "Amazon
#      Web Services" with an id of your choice, and set AWS_CONN_ID to that id.
#
# Whichever identity Airflow uses needs: ecs:RunTask on
# arn:aws:ecs:us-east-1:770724966330:task-definition/aws-duckdb-lakehouse-dev:*,
# iam:PassRole on the ecs_execution/ecs_task roles, plus ecs:DescribeTasks and
# logs:GetLogEvents to poll completion and stream logs into the Airflow UI —
# see infra/scheduler.tf's aws_iam_role_policy.scheduler for the exact policy
# (the local terraform-admin IAM user already has all of this via
# AdministratorAccess, so reusing its credentials is the simplest option).
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

AWS_REGION = "us-east-1"
AWS_CONN_ID = None  # or "aws_default" / your Connection id — see note above

ECS_CLUSTER = "aws-duckdb-lakehouse-dev"
ECS_TASK_DEFINITION = "aws-duckdb-lakehouse-dev"  # family only: always resolves to the latest ACTIVE revision
ECS_CONTAINER_NAME = "lakehouse"

# From `terraform output` in infra/ — the default VPC's subnets + security
# group (see infra/scheduler.tf's data sources). Update if you change these.
ECS_SUBNETS = [
    "subnet-0f913e9d8858518a8",
    "subnet-01f7d87850455c266",
]
ECS_SECURITY_GROUPS = ["sg-0c058ebff3381b6ad"]

CLOUDWATCH_LOG_GROUP = "/ecs/aws-duckdb-lakehouse/dev"
# ECS builds the actual stream name as "<prefix>/<container-name>/<task-id>"
# (see the awslogs-stream-prefix in infra/main.tf's container definition).
CLOUDWATCH_LOG_STREAM_PREFIX = f"lakehouse/{ECS_CONTAINER_NAME}"


def _run_task_operator(task_id: str, mode: str) -> EcsRunTaskOperator:
    return EcsRunTaskOperator(
        task_id=task_id,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        cluster=ECS_CLUSTER,
        task_definition=ECS_TASK_DEFINITION,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {"name": ECS_CONTAINER_NAME, "environment": [{"name": "MODE", "value": mode}]}
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ECS_SUBNETS,
                "securityGroups": ECS_SECURITY_GROUPS,
                "assignPublicIp": "ENABLED",
            }
        },
        awslogs_group=CLOUDWATCH_LOG_GROUP,
        awslogs_region=AWS_REGION,
        awslogs_stream_prefix=CLOUDWATCH_LOG_STREAM_PREFIX,
        wait_for_completion=True,
        waiter_delay=15,
        waiter_max_attempts=120,  # ~30min ceiling; the 5M-row orders model alone took ~9min on duckdb 1.5.5
    )


with DAG(
    dag_id="dbt_lakehouse",
    description="Runs the lakehouse dbt-build ECS task (bronze -> silver into S3 Tables)",
    schedule=None,  # trigger manually from the UI, or set a cron here if Airflow — not EventBridge Scheduler — should own the schedule
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=45),
    tags=["dbt", "lakehouse"],
) as dag:
    dbt_build = _run_task_operator("dbt_build", mode="dbt-build")
