import json
from typing import Dict, Optional, Union, Set
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pyarrow as pa
from typing import List
import pandas as pd
import io
import pyarrow.parquet as pq
from .igerenciador_arquivo import IGerenciadorArquivo
from ..cconfigs.cconfig import Cconfig
from airflow.utils.log.logging_mixin import LoggingMixin


class GerenciadorBucket(IGerenciadorArquivo):

    def __init__(self):

        self.__s3_hook = S3Hook(aws_conn_id=Cconfig.AWS_CON_ID)
        self.__NOME_BUCKET = Cconfig.NOME_BUCKET
        self.logger = LoggingMixin().log

    def abrir_arquivo(self, caminho_arquivo: str, ) -> Optional[Union[str, Dict, List, pa.Table]]:
        try:
            if caminho_arquivo.endswith(".parquet"):
                s3_key = self.__s3_hook.get_key(key=caminho_arquivo, bucket_name=self.__NOME_BUCKET)
                conteudo = s3_key.get()['Body'].read()
                buffer = io.BytesIO(conteudo)
                if buffer.getbuffer().nbytes == 0:
                    return None
                tabela = pq.read_table(buffer)
                self.logger.info(f"Tabela {caminho_arquivo} lida com sucesso")
                return tabela
            else:
                conteudo = self.__s3_hook.read_key(key=caminho_arquivo, bucket_name=self.__NOME_BUCKET)
                dados = json.loads(conteudo)
                return dados
        except Exception as e:
            self.logger.warning(f"Não foi possível ler {caminho_arquivo}: {e}")
            return None



    def guardar_arquivo(self, dado: Union[Dict, Set[str]], caminho_arquivo: str):
        conteudo_existente = self.abrir_arquivo(caminho_arquivo)
        if isinstance(dado, dict):
            lista_existente = []
            if conteudo_existente:
                if isinstance(conteudo_existente, list):
                    lista_existente = conteudo_existente
                elif isinstance(conteudo_existente, dict):
                    lista_existente = [conteudo_existente]

            lista_existente.append(dado)
            novo_json = json.dumps(lista_existente, ensure_ascii=False)
            self.__s3_hook.get_conn().put_object(
                Bucket=self.__NOME_BUCKET,
                Key=caminho_arquivo,
                Body=novo_json.encode("utf-8")
            )

        elif isinstance(dado, set):
            tabela_nova = pa.table({'valores': list(dado)})

            if conteudo_existente is not None:
                tabela = pa.concat_tables([conteudo_existente, tabela_nova])
                print(1)

                print(f'tabela {tabela}')
                print(f'tabela nova {tabela_nova}')
                print(f'conteudo_existente {conteudo_existente}')
            else:
                print(2)
                tabela = tabela_nova
                print(f'tabela {tabela}')
                print(f'tabela nova {tabela_nova}')
                print(f'conteudo_existente {conteudo_existente}')

            buffer = io.BytesIO()
            pq.write_table(tabela, buffer)
            buffer.seek(0)
            self.__s3_hook.get_conn().put_object(
                Bucket=self.__NOME_BUCKET,
                Key=caminho_arquivo,
                Body=buffer.read()
            )
