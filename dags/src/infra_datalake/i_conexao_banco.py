from abc import ABC, abstractmethod
import pandas as pd


class IConexaoBanco(ABC):

    @abstractmethod
    def criar_dados(self, texto: str) -> pd.DataFrame:
        pass
