# Primeira Dag com o Airflow
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args={
    'owner':'Logan',
    'depends_on_past': False,
    'start_date': datetime(2020,12,2,14),
    'email': ['loganmerazzi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

# Vamos definir a DAG
dag = DAG(
    "treino02",
    description='Extrai os dados do Titanic da internet e calcula a idade média',
    default_args=default_args,
    schedule_interval="*/2 * * * *"
        # timedelta(minutes=2) 
        # None para não fazer o schedule
        # "@once"
        # "@daily"
        # "@hourly"
)

get_data = BashOperator(
    task_id = 'get-data',
    bash_command = 'curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f"A idade média no Titanic era {value} anos.")

task_idade_media = PythonOperator(
    task_id = 'calcula-idade-media',
    python_callable = calculate_mean_age,
    dag=dag
)

task_print_idade = PythonOperator(
    task_id = 'mostra-idade',
    python_callable = print_age,
    provide_context=True,
    dag=dag
)

get_data >> task_idade_media >> task_print_idade