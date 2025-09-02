from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from src.api_steam.steam_reviews_api import SteamReviewsApi
from src.infra_datalake.gerenciador_bucket import GerenciadorBucket
from src.processo_etl.processo_etl import ProcessoETL


def executar_processo_etl_bronze(id_jogo: int):
    sra = SteamReviewsApi()
    dados = sra.obter_reviews_steam(codigo_jogo_steam=id_jogo, intervalo_dias=3)
    gb = GerenciadorBucket()
    data = datetime.now().date().strftime('%Y_%m_%d')
    caminho = f'datalake/bronze/data_{data}/jogo_{id_jogo}/reviews.json'
    print(caminho)
    e = ProcessoETL(texto='a')
    for dado in dados:
        print(dado)
        gb.guardar_arquivo(dado=dado, caminho_arquivo=caminho)



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
    with TaskGroup('task_steam_comentarios_bronze', dag=dag) as tg_steam_bronze:
        lista_tasks = []
        for i in lista_ids:
            salvar_camada_bronze = PythonOperator(
                task_id=f"executar_processo_etl_bronze_{i}",
                python_callable=executar_processo_etl_bronze,
                op_kwargs={"id_jogo": i}
            )
            lista_tasks.append(lista_ids)

    fim_dag = EmptyOperator(task_id="fim_dag", trigger_rule='all_done')

    inicio_dag >> checar_url_minio >> salvar_camada_bronze >> fim_dag
    checar_url_minio >> dag_erro >> fim_dag
