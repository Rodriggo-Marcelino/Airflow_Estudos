import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='dagrundag_2',
    description='DAG que aciona outro DAG',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command="echo 'DAG 2 executada com sucesso!'",
    )
    
    task2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 5',
    )
    
    task1 >> task2