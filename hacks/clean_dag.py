from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

default_args = {
    'owner': 'airflow',
    "description": "Use of the DockerOperator",
    'depends_on_past': False,
    "start_date": datetime(2021, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'capstone_llm_clean',
    default_args=default_args,
    schedule="5 * * * *",
    catchup=False,
) as dag:

    clean_task = DockerOperator(
        task_id='clean_task',
        image='capstone-llm-clean:latest',
        container_name='capstone_llm_clean_task',
        api_version='auto',
        auto_remove='force',
        command='python -m capstonellm.tasks.clean -t docker',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
            'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            'AWS_DEFAULT_REGION': 'us-east-1',
        },
    )
