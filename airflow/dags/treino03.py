import pandas as pd
import random as rnd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
    "treino03",
    description='Pega os dados do Titanic da internet e calcula a idade média para homens ou mulheres',
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

def sorteia_h_m():
    return rnd.choice(['male','female'])

escolhe_h_m = PythonOperator(
    task_id='escolhe-h-m',
    python_callable=sorteia_h_m,
    dag=dag
)

def MouF(**context):
    value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
    if value =='male':
        return 'branch_homem'
    else:
        return 'branch_mulher'

male_female = BranchPythonOperator(
    task_id = 'condicional',
    python_callable=MouF,
    provide_context=True,
    dag=dag
)

def mean_homem():
    df = pd.read_csv('~/train.csv')
    df = df.loc[df.Sex == 'male']
    print(f"Media de idade dos homens no Titanic: {df.Age.mean()}")

branch_homem = PythonOperator(
    task_id='branch_homem',
    python_callable=mean_homem,
    dag=dag
)

def mean_mulher():
    df = pd.read_csv('~/train.csv')
    df = df.loc[df.Sex == 'female']
    print(f"Media de idade das mulheres no Titanic: {df.Age.mean()}")

branch_mulher = PythonOperator(
    task_id='branch_mulher',
    python_callable=mean_mulher,
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f"A idade média no Titanic era {value} anos.")

get_data >> escolhe_h_m >> male_female >> [branch_homem, branch_mulher]
