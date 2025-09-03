from abc import ABC, abstractmethod
import pandas as pd


class IConexaoBanco(ABC):

    @abstractmethod
    def conectar_banco(self):
        pass

    @abstractmethod
    def criar_dados(self) -> pd.DataFrame:
        pass
