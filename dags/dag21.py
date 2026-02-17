import pendulum
import random       
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, ShortCircuitOperator


with DAG(
    
    dag_id='curto_circuito',
    description='Testando curto circuito no airflow',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
    
) as dag:

    def gera_qualidade() -> int:
        return random.randint(1,100)
   
    gera_qualidade_task = PythonOperator(
        task_id = 'gera_qualidade',
        python_callable=gera_qualidade
    )
    
    def verifica_qualidade() -> bool:
        context = get_current_context()
        qualidade = context['ti'].xcom_pull(task_ids='gera_qualidade')
        if qualidade >= 70:
            return True
        else:
            return False
        
    verifica_qualidade_task = ShortCircuitOperator(
        task_id = 'verifica_qualidade',
        python_callable=verifica_qualidade
    )
   
   
    printa_qualidade_boa_task = BashOperator(
        task_id = 'printa_qualidade_boa',
        bash_command='echo "A qualidade é boa!"'
    )
    
    
    printa_qualidade_ruim_task = BashOperator(
        task_id = 'finaliza_qualidade_ruim',
        bash_command='echo "A qualidade é ruim!"'
    )
    
    gera_qualidade_task >> verifica_qualidade_task >> printa_qualidade_boa_task
    verifica_qualidade_task >> printa_qualidade_ruim_task
    