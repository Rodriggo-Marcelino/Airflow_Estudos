import pendulum
from airflow import DAG
from airflow.sdk import task
with DAG(
    dag_id='Xcom_dag',
    description='DAG de exemplo para XComs',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo', 'xcom'],
) as dag:
  
    @task
    def task_write():
      return {'message': 'Hello from XCom!', 'value': 42}
  
    @task
    def task_read(data):
      message = data['message']
      value = data['value']
      print(f"Received message: {message}")
      print(f"Received value: {value}")

    task_read(task_write())