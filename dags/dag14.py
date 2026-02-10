import pendulum
from airflow import DAG
from airflow.operators.email import EmailOperator

with DAG(
    dag_id='Testando_email',
    description='Exemplo de DAG utilizando o EmailOperator',
    schedule='@once',  
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['curso', 'exemplo', 'email'],
) as dag:

    EmailOperator(
        task_id='send_email',
        to='example@example.com',
        subject='Test Email from Airflow',
        html_content='<h3>This is a test email sent from Airflow using EmailOperator</h3>',
    )
