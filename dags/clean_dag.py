from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
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
    description='Clean and join StackOverflow questions and answers for LLM training',
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 2, 5),
    catchup=False,
    tags=['capstone', 'llm', 'stackoverflow', 'clean'],
) as dag:

    clean_dbt_task = BashOperator(
        task_id='clean_sql_data',
        bash_command=f'''docker run --rm \
            -e AWS_ACCESS_KEY_ID={os.environ.get('AWS_ACCESS_KEY_ID', '')} \
            -e AWS_SECRET_ACCESS_KEY={os.environ.get('AWS_SECRET_ACCESS_KEY', '')} \
            -e AWS_DEFAULT_REGION=us-east-1 \
            capstone-llm-clean:latest \
            python -m capstonellm.tasks.clean -t pyspark -e production
        ''',
    )
