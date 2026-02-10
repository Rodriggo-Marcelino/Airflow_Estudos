import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator   
from airflow.models import Variable


with DAG(
    
    dag_id='var_dag',
    description='DAG de exemplo utilizando o Variable',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo'],
    
) as dag:

    task1 = PythonOperator(
        task_id='task_1',
        python_callable=lambda: print(Variable.get('minhavar')),
    )
   
    