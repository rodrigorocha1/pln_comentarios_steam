import spacy


class ProcessoETL:
    def __init__(self, texto: str):
        self.__nlp = spacy.load("pt_core_news_sm")
        self.__doc = self.__nlp(texto)

    def a(self):
        pass
