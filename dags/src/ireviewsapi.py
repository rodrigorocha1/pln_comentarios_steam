from abc import ABC, abstractmethod
from typing import Dict, Generator


class IReviewsAPI(ABC):
    @abstractmethod
    def obter_reviews_steam(self, codigo_jogo_steam: int, intervalo_dias: int) -> Generator[Dict, None, None]:
        """
        Método para obter as reviews da steam
        :param codigo_jogo_steam: código do jogo da steam
        :type codigo_jogo_steam: int
        :param intervalo_dias: intervalo de buscas
        :type intervalo_dias: int
        :return: Gerador com as reviews
        :rtype:  Generator[Dict, None, None]
        """
        pass
