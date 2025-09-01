from airflow import DAG
from airflow.decorators import task, task_group

# Define o DAG
with DAG(
        dag_id="exemplo_taskgroup_decorador",
        catchup=False,
) as dag:
    @task
    def start():
        return "Iniciando"


    @task
    def process_data(data):
        return f"Processando {data}"


    @task
    def end(data):
        print(f"Fim: {data}")


    # TaskGroup como decorador
    @task_group(group_id="grupo_processamento")
    def processamento_group():
        data1 = process_data("dado1")
        data2 = process_data("dado2")
        return [data1, data2]


    # Orquestração das tarefas
    inicio = start()
    resultados = processamento_group()
    fim = end(resultados)

    inicio >> resultados >> fim
