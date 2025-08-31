from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from src.infra_datalake.gerenciador_bucket import GerenciadorBucket


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


    @task
    def guardar_dados():
        # Inicializa o gerenciador da camada 'bronze'
        gb = GerenciadorBucket(camada='bronze')
        # Exemplo de dados
        dados = {'a': 1, 'b': 2}
        # Caminho dentro do bucket
        caminho = 'datalake/bronze/exemplo.json'
        # Salva no S3
        gb.guardar_arquivo(dado=dados, caminho_arquivo=caminho)
        return f"Arquivo salvo em {caminho}"


    salvar = guardar_dados()




    falha_dag = EmptyOperator(task_id="falha_dag", trigger_rule="one_failed")
    fim_dag = EmptyOperator(task_id="fim_dag", trigger_rule="all_done")


    # Definindo dependências
    inicio_dag >> checar_url_minio >> salvar >> fim_dag
    checar_url_minio >> falha_dag >> fim_dag
