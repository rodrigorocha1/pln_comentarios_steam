import json
from typing import Dict, Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.src.infra_datalake.igerenciador_arquivo import IGerenciadorArquivo
from dags.cconfigs.cconfig import Cconfig


class GerenciadorBucket(IGerenciadorArquivo):

    def __init__(self, camada: str):
        self.__camada = camada
        self.__s3_hook = S3Hook(aws_conn_id=Cconfig.AWS_CON_ID)
        self.__NOME_BUCKET = Cconfig.NOME_BUCKET

    def abrir_arquivo(self, caminho_arquivo: str, ) -> Optional[str]:
        try:
            conteudo_existente = self.__s3_hook.read_key(
                key=caminho_arquivo,
                bucket_name=self.__NOME_BUCKET
            )
        except:
            conteudo_existente = None
        return conteudo_existente

    def guardar_arquivo(self, dado: Dict, caminho_arquivo: str):
        novo_json = json.dumps(dado, indent=4)
        conteudo_existente = self.abrir_arquivo(caminho_arquivo=caminho_arquivo)
        if conteudo_existente:
            novo_json = conteudo_existente + novo_json + "\n"
        self.__s3_hook.load_file(
            string_data=novo_json,
            bucket_name=self.__NOME_BUCKET,
            key=caminho_arquivo,

        )
