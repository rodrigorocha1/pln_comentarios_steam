from src.api_steam.steam_reviews_api import SteamReviewsApi
from ..infra_datalake.gerenciador_bucket import GerenciadorBucket
from ..infra_datalake.conexao_banco import ConexaoBanco
import spacy




def executar_processo_etl_bronze(id_jogo: int, data: str):
    sra = SteamReviewsApi()
    dados = sra.obter_reviews_steam(codigo_jogo_steam=id_jogo, intervalo_dias=3)
    gb = GerenciadorBucket()
    caminho = f'datalake/bronze/data_{data}/jogo_{id_jogo}/reviews.json'
    for dado in dados:
        print(dado)
        gb.guardar_arquivo(dado=dado, caminho_arquivo=caminho)


def executar_processo_etl_prata(id_jogo: int, data: str):
    gb = GerenciadorBucket()
    caminho = f'datalake/bronze/data_{data}/jogo_{id_jogo}/reviews.json'
    dados = gb.abrir_arquivo(caminho_arquivo=caminho)
    conexao_banco = ConexaoBanco(caminho=caminho)
    dataframe = conexao_banco.consultar_dados()
    print(dataframe)
