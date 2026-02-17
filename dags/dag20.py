import pendulum
import random       
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator


with DAG(
    
    dag_id='branchcast',
    description='Testando branchcast no airflow',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
    
) as dag:

    def gera_numero_aleatorio():
        return random.randint(1,100)
   
    gera_numero_aleatorio_task = PythonOperator(
        task_id = 'gera_numero_aleatorio',
        python_callable=gera_numero_aleatorio
    )
    
    def verifica_par_impar():
        context = get_current_context()
        numero_aleatorio = context['ti'].xcom_pull(task_ids='gera_numero_aleatorio')
        if numero_aleatorio % 2 == 0:
            return 'numero_par'
        else:
            return 'numero_impar'
    
    verifica_par_impar_task = BranchPythonOperator(
        task_id = 'verifica_par_impar',
        python_callable=verifica_par_impar
    )
    
    numero_par_task = BashOperator(
        task_id = 'numero_par',
        bash_command='echo "O número é par!"'
    )   
    
    numero_impar_task = BashOperator(
        task_id = 'numero_impar',
        bash_command='echo "O número é ímpar!"'
    )
    
    
    gera_numero_aleatorio_task >> verifica_par_impar_task >> [numero_par_task, numero_impar_task]
   
    