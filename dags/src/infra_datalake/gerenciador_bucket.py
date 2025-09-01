import json
from typing import Dict, Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from .igerenciador_arquivo import IGerenciadorArquivo
from ..cconfigs.cconfig import Cconfig


class GerenciadorBucket(IGerenciadorArquivo):

    def __init__(self):

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
        novo_json = json.dumps(dado)
        conteudo_existente = self.abrir_arquivo(caminho_arquivo=caminho_arquivo)
        if conteudo_existente:
            novo_json = conteudo_existente.rstrip("\n") + "\n" + novo_json
        self.__s3_hook.get_conn().put_object(
            Bucket=self.__NOME_BUCKET,
            Key=caminho_arquivo,
            Body=novo_json.encode("utf-8")
        )


if __name__ == '__main__':
    gb = GerenciadorBucket(camada='bronze')
    dados = {'a': 1, 'b': 2}
    gb.guardar_arquivo(dado=dados, caminho_arquivo='meu-bucket/datalake/bronze')
