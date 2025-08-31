from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Task Python usando o decorator do Airflow 3.x
@task
def listar_arquivos():
    hook = S3Hook(aws_conn_id="teste")
    arquivos = hook.list_keys(bucket_name="meu-bucket")
    print(f"Arquivos encontrados: {arquivos}")
    return arquivos


with DAG(
        dag_id="exemplo_empty_operator",
        start_date=datetime(2025, 8, 31),

        catchup=False,
        tags={"exemplo", "s3", "minio"},
) as dag:
    inicio_dag = EmptyOperator(task_id="inicio_dag")

    checar_url_minio = HttpOperator(
        task_id='minio_http',
        method='GET',
        http_conn_id='minio_http',  # CORRETO
        endpoint='minio/health/ready',
        response_check=lambda response: response.status_code == 200,
        log_response=True,

    )




    falha_dag = EmptyOperator(task_id="falha_dag", trigger_rule="one_failed")
    fim_dag = EmptyOperator(task_id="fim_dag", trigger_rule="all_done")


    # Definindo dependências
    inicio_dag >> checar_url_minio >> fim_dag
    checar_url_minio >> falha_dag >> fim_dag
