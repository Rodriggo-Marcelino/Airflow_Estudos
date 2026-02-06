import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='dagrundag_1',
    description='DAG que aciona outro DAG',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5',
    )
    
    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='dagrundag_2',  # ID do DAG que será acionado
        conf= {"message": "Hello from DAG 1!"},  # Dados que podem ser passados para o DAG acionado
        wait_for_completion=True,  # Espera o DAG acionado finalizar antes de continuar
        poke_interval=5,  # Intervalo em segundos para verificar o status do DAG acionado
    )
    
    task1 >> trigger_dag2