# Primeira Dag com o Airflow

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
    description='Básico de bash e Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# Vamos adicionar as tarefas
hello_bash = BashOperator(
    task_id="Hello_Bash",
    bash_command='echo "Hello airflow from bash!"',
    dag=dag
)

def run_hello_python():
    print ("Hello airflow from python")

hello_python = PythonOperator(
    task_id="Hello_Python",
    python_callable=run_hello_python,
    dag=dag
)

hello_bash >> hello_python