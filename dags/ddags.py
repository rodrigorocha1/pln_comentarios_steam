from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
dag = DAG(
    'example_bash_jinja_templates',
    default_args=default_args,
    description='Exemplo de BashOperator com várias variáveis de template',
    start_date=datetime(2025, 8, 31),
    catchup=False,
    tags={'a'} ,
    
)

# Task 1: ds
t_ds = BashOperator(
    task_id="print_ds",
    bash_command="echo ds: {{ ds }}",
    dag=dag,
)

# Task 2: ds_nodash
t_ds_nodash = BashOperator(
    task_id="print_ds_nodash",
    bash_command="echo ds_nodash: {{ ds_nodash }}",
    dag=dag,
)

# Task 3: ts
t_ts = BashOperator(
    task_id="print_ts",
    bash_command="echo ts: {{ ts }}",
    dag=dag,
)

# Task 4: ts_nodash
t_ts_nodash = BashOperator(
    task_id="print_ts_nodash",
    bash_command="echo ts_nodash: {{ ts_nodash }}",
    dag=dag,
)

# Task 5: ts_nodash_with_tz
t_ts_nodash_tz = BashOperator(
    task_id="print_ts_nodash_with_tz",
    bash_command="echo ts_nodash_with_tz: {{ ts_nodash_with_tz }}",
    dag=dag,
)

# Dependências (executa em sequência)
t_ds >> t_ds_nodash >> t_ts >> t_ts_nodash >> t_ts_nodash_tz
