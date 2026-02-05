from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'capstone_llm_clean',
    default_args=default_args,
    description='Clean questions and answers',
    schedule=None,
    start_date=datetime(2026, 2, 5),
    catchup=False,
) as dag:

    clean_task = DockerOperator(
        task_id='clean',
        image='capstone-llm-clean:latest',
        container_name='capstone_llm_clean_task',
        api_version='auto',
        auto_remove='force',
        command='python -m capstonellm.tasks.clean -t pyspark -e production',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
            'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            'AWS_DEFAULT_REGION': 'us-east-1',
        },
    )
