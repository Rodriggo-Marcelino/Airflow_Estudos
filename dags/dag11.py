import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context

with DAG(
    dag_id='Xcom_dag_2',
    description='DAG de exemplo para XComs 2',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo', 'xcom'],
) as dag:

    def task_write():
        context = get_current_context()
        ti = context['ti']
        ti.xcom_push(key='mensagem', value='Olá do XCom!')
    
    def task_read():
        context = get_current_context()
        ti = context['ti']
        mensagem = ti.xcom_pull(key='mensagem', task_ids='write_task')
        print(f'Mensagem recebida do XCom: {mensagem}')
        
    task1 = PythonOperator(
        task_id='write_task',
        python_callable=task_write,
    )
    
    task2 = PythonOperator(
        task_id='read_task',
        python_callable=task_read,
    )
    task1 >> task2