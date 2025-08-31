from abc import ABC, abstractmethod
from typing import Dict


class IGerenciadorArquivo(ABC):

    @abstractmethod
    def guardar_arquivo(self, dado: Dict, caminho_arquivo: str):
        pass
