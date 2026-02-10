import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator   

with DAG(
    
    dag_id='empty_dag',
    description='DAG de exemplo utilizando o EmptyOperator',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
    
) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5',
    )
   
    task2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 5',
    )
   
    task3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 5',
    )
   
    task4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 5',
    )
   
    task5 = BashOperator(
        task_id='task_5',
        bash_command='sleep 5',
    )
    
    taskempty = EmptyOperator(
        task_id='task_empty',
    )
    
    [task1,task2,task3] >> taskempty >> [task4,task5]