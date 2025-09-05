from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from src.processo_etl.processo_etl import ProcessoEtl

data = datetime.now().date().strftime('%Y_%m_%d')

lista_ids = [244850, 275850]

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

    with TaskGroup('task_steam_reviews_bronze', dag=dag) as tg_steam_bronze:
        lista_tasks = []
        for i in lista_ids:
            salvar_camada_bronze = PythonOperator(
                task_id=f"executar_processo_etl_bronze_{i}",
                python_callable=ProcessoEtl(caminho=None).executar_processo_etl_bronze,
                op_kwargs={"id_jogo": i, "data": data}
            )
            lista_tasks.append(lista_ids)

    with TaskGroup('task_steam_reviews_prata', dag=dag) as tg_steam_prata:
        lista_tasks = []
        for i in lista_ids:
            salvar_camada_prata = PythonOperator(
                task_id=f"executar_processo_etl_prata_{i}",
                python_callable=ProcessoEtl(caminho=None).executar_processo_etl_prata_comentarios_tratados,
                op_kwargs={"id_jogo": i, "data": data}

            )
            lista_tasks.append(salvar_camada_prata)

    with TaskGroup('task_steam_reviews_prata_comentario_tratado', dag=dag) as tg_steam_prata_comentarios_bruto:
        lista_tasks = []
        for i in lista_ids:
            salvar_camada_prata_comentarios_brutos = PythonOperator(
                task_id=f"executar_processo_etl_prata_cb_{i}",
                python_callable=ProcessoEtl(caminho=None).executar_processo_etl_prata_comentarios_brutos,
                op_kwargs={"id_jogo": i, "data": data}
            )

    fim_dag = EmptyOperator(task_id="fim_dag", trigger_rule='all_done')

    inicio_dag >> checar_url_minio >> tg_steam_bronze >> tg_steam_prata >> tg_steam_prata_comentarios_bruto >> fim_dag

    checar_url_minio >> dag_erro >> fim_dag
