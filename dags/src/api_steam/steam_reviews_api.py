from typing import Dict, Generator
from ..cconfigs.cconfig import Cconfig
import requests
import urllib.parse
from .ireviewsapi import IReviewsAPI



class SteamReviewsApi(IReviewsAPI):

    def __init__(self):
        self.__URL = Cconfig.URL_STEAM

    def obter_reviews_steam(self, codigo_jogo_steam: int, intervalo_dias) -> Generator[Dict, None, None]:
        """
        Método para obter as reviews da steam
        :param codigo_jogo_steam: código do jogo da steam
        :type codigo_jogo_steam: int
        :param intervalo_dias: intervalo de buscas
        :type intervalo_dias: int
        :return: Gerador com as reviews
        :rtype:  Generator[Dict, None, None]
        """
        parametros = {
            'json': 1,
            'filter': 'recent',
            'language': 'portuguese',
            'cursor': '*',
            'review_type': 'all',
            'purchase_type': 'all',
            'num_per_page': '100',
            'day_range': intervalo_dias
        }
        while True:
            if parametros['cursor'] is None:
                break
            url_api = f'{self.__URL}{codigo_jogo_steam}'
            req = requests.get(url=url_api, params=parametros, timeout=10)
            req = req.json()
            yield from req['reviews']
            cursor = req.get('cursor')
            if not cursor:
                break
            cursor = cursor.strip('"')
            parametros['cursor'] = urllib.parse.quote(cursor)


if __name__ == '__main__':
    sra = SteamReviewsApi()

    dados = sra.obter_reviews_steam(codigo_jogo_steam=244850, intervalo_dias=10)
    for dado in dados:
        print(dado['review'])
