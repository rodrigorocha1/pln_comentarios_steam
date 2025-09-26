from typing import List, Tuple

import duckdb
import spacy
from spacy.tokens import Doc

from dags.src.infra_datalake.gerenciador_bucket import GerenciadorBucket


class ProcessoEtlOuro:
    def __init__(self, id_jogo: int, opcao: int):
        self.__AWS_ACCESS_KEY_ID = "meuusuario"
        self.__AWS_SECRET_ACCESS_KEY = "minhasenha123"
        self.__AWS_ENDPOINT_URL = "172.40.0.21:9000"
        self.__BUCKET_NAME = "meu-bucket"
        self.__ARQUIVO_PATH = f"datalake/prata/data_*/jogo_{id_jogo}/reviews.parquet" if opcao == 1 else f"datalake/prata/comentarios_refinados/jogo_{id_jogo}/reviews_refinados.parquet"
        self.__bucket_ouro = GerenciadorBucket()
        self.__id_jogo = id_jogo

    def __recuperar_dados(self) -> List[str]:
        con = duckdb.connect('dados_reviews/reviews.duckdb')
        con.execute(f"SET s3_access_key_id='{self.__AWS_ACCESS_KEY_ID}';")
        con.execute(f"SET s3_secret_access_key='{self.__AWS_SECRET_ACCESS_KEY}';")
        con.execute(f"SET s3_endpoint='{self.__AWS_ENDPOINT_URL}';")
        con.execute("SET s3_region='us-east-1';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

        df = con.execute(f"""
            SELECT * FROM read_parquet('s3://{self.__BUCKET_NAME}/{self.__ARQUIVO_PATH}')
        """).df()

        lista = df['valores'].tolist()
        return lista

    def __carregar_texto(self, lista: List[str]) -> Tuple[str, List[Doc]]:
        nlp = spacy.load("pt_core_news_sm")
        docs = list(nlp.pipe(lista))
        texto = ' '.join(lista)
        return texto, docs

    def salvar_wordcloud(self):
        dados = self.__recuperar_dados()
        texto = self.__carregar_texto(lista=dados)[0]
        caminho = caminho = f"datalake/ouro/jogo_{self.__id_jogo}/wordcloud.png"
        self.__bucket_ouro.salvar_wordcloud(texto=texto, caminho_arquivo=caminho)