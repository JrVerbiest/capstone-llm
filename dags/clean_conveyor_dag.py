from datetime import datetime, timedelta

from airflow import DAG
from conveyor.operators import (
    ConveyorContainerOperatorV2,
    ConveyorSparkSubmitOperatorV2,
)

default_args = {
    "owner": "airflow",
    "description": "Clean questions and answers",
    "depend_on_past": False,
    "start_date": datetime(2026, 2, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstone_llm_clean_conveyor",
    default_args=default_args,
    description="Clean questions and answers using Conveyor",
    schedule="5 * * * *",
    catchup=False,
) as dag:
    clean_task = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="clean",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        cmds=["python3"],
        arguments=["-m", "capstonellm.tasks.clean", "-t", "docker", "-e", "prod"],
        env_vars={
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )