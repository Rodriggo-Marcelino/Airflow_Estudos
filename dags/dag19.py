import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable


with DAG(
    
    dag_id='teste_de_pools',
    description='Testando o uso de pools no Airflow',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
    
) as dag:

    task_leve = BashOperator(
        task_id='task_1',
        bash_command='sleep 5',
        pool='meupool',
        priority_weight=1,
        weight_rule='absolute',
    )
    
    task_media = BashOperator(
        task_id='task_2',
        bash_command='sleep 5',
        pool='meupool',
        priority_weight=5,
        weight_rule='absolute',
    )
    
    task_pesada = BashOperator(
        task_id='task_3',
        bash_command='sleep 5',
        pool_slots=2,
        pool='meupool', 
        priority_weight=10,
        weight_rule='absolute',
    )
   
    
   
    