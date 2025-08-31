from abc import ABC, abstractmethod
from typing import Dict, Optional


class IGerenciadorArquivo(ABC):

    @abstractmethod
    def abrir_arquivo(self, caminho_arquivo: str, ) -> Optional[str]:
        pass

    @abstractmethod
    def guardar_arquivo(self, dado: Dict, caminho_arquivo: str):
        pass
