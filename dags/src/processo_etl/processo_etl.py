from ..api_steam.steam_reviews_api import SteamReviewsApi
from ..infra_datalake.gerenciador_bucket import GerenciadorBucket
from ..infra_datalake.conexao_banco import ConexaoBanco
from typing import Optional
from langdetect import detect, DetectorFactory
import unicodedata
import spacy
import re
from pandas import json_normalize


class ProcessoEtl:
    def __init__(self, caminho: Optional[str]):
        self.__steam_api = SteamReviewsApi()
        self.__gerenciador_bk = {
            'bronze': GerenciadorBucket(),
            'prata': GerenciadorBucket()
        }
        self.__conexao_banco = ConexaoBanco(caminho=caminho)
        self.__nlp = spacy.load("pt_core_news_sm")
        DetectorFactory.seed = 0

    def remover_acentos(self, texto):

        return ''.join(
            c for c in unicodedata.normalize('NFKD', texto)
            if not unicodedata.combining(c)
        )

    def fazer_preprocessamento(self, texto):
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # símbolos e pictogramas
                                   u"\U0001F680-\U0001F6FF"  # transporte e símbolos
                                   u"\U0001F1E0-\U0001F1FF"  # bandeiras
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   "]+", flags=re.UNICODE)
        texto = re.sub(r'\s+', ' ', texto)
        texto = emoji_pattern.sub(r'', texto)  # remove emojis
        doc = self.__nlp(texto)
        tokens_processados = set()
        for token in doc:
            if not token.is_stop and not token.is_punct and not token.is_digit and not token.is_space:
                token_sem_acento = self.remover_acentos(token.lemma_.lower())
                tokens_processados.add(token_sem_acento)
        return tokens_processados

    def is_portuguese(self, text):
        try:
            return detect(text) == 'pt'
        except:
            return False

    def executar_processo_etl_bronze(self, id_jogo: int, data: str):
        dados = self.__steam_api.obter_reviews_steam(codigo_jogo_steam=id_jogo, intervalo_dias=3)
        caminho = f'datalake/bronze/data_{data}/jogo_{id_jogo}/reviews.jsonl'
        for dado in dados:
            self.__gerenciador_bk['bronze'].guardar_arquivo(dado=dado, caminho_arquivo=caminho)

    def executar_processo_etl_prata(self, id_jogo: int, data: str):
        caminho_bronze = f'datalake/bronze/data_{data}/jogo_{id_jogo}/reviews.jsonl'
        caminho_prata = f'datalake/prata/data_{data}/jogo_{id_jogo}/reviews.parquet'
        dados = self.__gerenciador_bk['bronze'].abrir_arquivo(caminho_arquivo=caminho_bronze)
        if dados is None:
            dados_normalizados = []
        elif isinstance(dados, dict):
            dados_normalizados = [dados]
        elif isinstance(dados, list):
            dados_normalizados = [d for d in dados if isinstance(d, dict)]
        else:
            raise TypeError(f"Tipo inesperado em 'dados': {type(dados)}")
        dataframe = json_normalize(dados_normalizados, sep='_')
        dataframe['portugues'] = dataframe['review'].apply(self.is_portuguese)
        dataframe = dataframe[dataframe['portugues']]
        tupla_de_linhas = dataframe['review'].tolist()
        texto_completo = ' '.join(tupla_de_linhas)
        texto_tratado = self.fazer_preprocessamento(texto=texto_completo)
        self.__gerenciador_bk['prata'].guardar_arquivo(dado=texto_tratado, caminho_arquivo=caminho_prata)
