import pendulum
from airflow import DAG
from datetime import timedelta  
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='default_args_dag',
    description='DAG com default_args no Airflow',
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['args', 'exemplo pratico'],
) as dag:
    
    task1 = BashOperator(
        task_id='tsk1',
        bash_command='exit 1',  # Comando que falha para testar retries
        retries = 3  # Sobrescreve o valor padrão de retries
    )

    task2 = BashOperator(
        task_id='tsk2',
        bash_command='sleep 5'
    )

    task3 = BashOperator(
        task_id='tsk3',
        bash_command='sleep 5'
    )

    task1 >> task2 >> task3
