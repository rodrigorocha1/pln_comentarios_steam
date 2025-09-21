from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.task_group import TaskGroup

from src.processo_etl.processo_etl import ProcessoEtl

# IDs de jogos
lista_ids = [244850, 275850, 359320, 392160, 1623730, 255710, 949230]


def run_bronze(id_jogo, data):
    ProcessoEtl(caminho=None).executar_processo_etl_bronze(id_jogo=id_jogo, data=data)

def run_prata(id_jogo, data):
    ProcessoEtl(caminho=None).executar_processo_etl_prata_comentarios_tratados(id_jogo=id_jogo, data=data)

def run_prata_cb(id_jogo, data):
    ProcessoEtl(caminho=None).executar_processo_etl_prata_comentarios_brutos(id_jogo=id_jogo, data=data)


with DAG(
        dag_id="dag_comentarios_steam",
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:
    inicio_dag = EmptyOperator(task_id="inicio_dag")

    checar_url_minio = HttpOperator(
        task_id='minio_http',
        method='GET',
        http_conn_id='minio_http',
        endpoint='minio/health/ready',
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    dag_erro = EmptyOperator(task_id='dag_erro', trigger_rule='one_failed')

    with TaskGroup('task_steam_reviews_bronze') as tg_steam_bronze:
        for i in lista_ids:
            PythonOperator(
                task_id=f"executar_processo_etl_bronze_{i}",
                python_callable=run_bronze,
                op_kwargs={"id_jogo": i, "data": "{{ ds_nodash }}"}
            )

    with TaskGroup('task_steam_reviews_prata') as tg_steam_prata:
        for i in lista_ids:
            PythonOperator(
                task_id=f"executar_processo_etl_prata_{i}",
                python_callable=run_prata,
                op_kwargs={"id_jogo": i, "data": "{{ ds_nodash }}"}
            )

    with TaskGroup('task_steam_reviews_prata_comentario_tratado') as tg_steam_prata_comentarios_bruto:
        for i in lista_ids:
            PythonOperator(
                task_id=f"executar_processo_etl_prata_cb_{i}",
                python_callable=run_prata_cb,
                op_kwargs={"id_jogo": i, "data": "{{ ds_nodash }}"}
            )

    fim_dag = EmptyOperator(task_id="fim_dag", trigger_rule='all_done')

    inicio_dag >> checar_url_minio >> tg_steam_bronze >> tg_steam_prata >> tg_steam_prata_comentarios_bruto >> fim_dag
    checar_url_minio >> dag_erro >> fim_dag
