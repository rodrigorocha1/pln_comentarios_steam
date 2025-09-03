import duckdb
import pandas as pd
from ..infra_datalake.i_conexao_banco import IConexaoBanco
from typing import Optional


class ConexaoBanco(IConexaoBanco):

    def __init__(self, caminho: Optional[str]):
        self.__AWS_ACCESS_KEY_ID = "meuusuario"
        self.__AWS_SECRET_ACCESS_KEY = "minhasenha123"
        self.__AWS_ENDPOINT_URL = "172.40.0.21:9000"
        self.__BUCKET_NAME = "meu-bucket"
        self.__arquivo_path = caminho

    @property
    def arquivo_path(self) ->  Optional[str]:
        return self.__arquivo_path

    @arquivo_path.setter
    def arquivo_path(self, arquivo_path: str):
        self.__arquivo_path = arquivo_path

    def conectar_banco(self):
        con = duckdb.connect(":memory:")
        con.execute(f"SET s3_access_key_id='{self.__AWS_ACCESS_KEY_ID}';")
        con.execute(f"SET s3_secret_access_key='{self.__AWS_SECRET_ACCESS_KEY}';")
        con.execute(f"SET s3_endpoint='{self.__AWS_ENDPOINT_URL}';")
        con.execute("SET s3_region='us-east-1';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")
        return con

    def criar_dados(self, funcao) -> pd.DataFrame:
        dataframe = self.conectar_banco().execute(f"""
                  SELECT * 
                  FROM {funcao}('s3://{self.__BUCKET_NAME}/{self.__arquivo_path}')
              """).df()
        return dataframe
