import duckdb
from nltk.util import ngrams
from collections import Counter
from langdetect import detect, DetectorFactory
from spacy import displacy
import spacy
import unicodedata
import re
import pathlib
from nltk import FreqDist
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib

# Fonte padrão do matplotlib (TrueType)

id_jogo = '244850'
#  read_json_auto
AWS_ACCESS_KEY_ID = "meuusuario"
AWS_SECRET_ACCESS_KEY = "minhasenha123"
AWS_ENDPOINT_URL = "172.40.0.21:9000"  # host:porta
BUCKET_NAME = "meu-bucket"
ARQUIVO_PATH = f"datalake/prata/data_*/jogo_{id_jogo}/reviews.parquet"
#ARQUIVO_PATH_COMENTARIOS_BRUTOS = f"datalake/prata/comentarios_brutos/jogo_{id_jogo}/reviews_bruto.parquet"
ARQUIVO_PATH_COMENTARIOS_BRUTOS = f"datalake/prata/comentarios_refinados/jogo_{id_jogo}/reviews_refinados.parquet"


con = duckdb.connect('dados_reviews/reviews.duckdb')
con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
con.execute(f"SET s3_endpoint='{AWS_ENDPOINT_URL}';")
con.execute("SET s3_region='us-east-1';")  # DuckDB exige, mesmo que MinIO ignore
con.execute("SET s3_use_ssl=false;")  # usa HTTP
con.execute("SET s3_url_style='path';")  # estilo path obrigatório para MinIO

df = con.execute(f"""
    SELECT * FROM read_parquet('s3://{BUCKET_NAME}/{ARQUIVO_PATH}')
""").df()
df_brutos = con.execute(f"""
    SELECT * FROM read_parquet('s3://{BUCKET_NAME}/{ARQUIVO_PATH_COMENTARIOS_BRUTOS}')
""").df()
lista = df['valores'].tolist()
lista.sort()
print(lista)

nlp = spacy.load("pt_core_news_sm")
docs = list(nlp.pipe(lista))
texto = ' '.join(lista)
print(texto)
# 1- wordcloud
print('1- wordcloud')
wc = WordCloud(width=1200, height=600, background_color="white",
               collocations=False)
img = wc.generate(texto)

#  2 -Mostrar
print(' 2 -Mostrar')
plt.figure(figsize=(12, 6))
plt.imshow(img)
plt.axis("off")
plt.title("WordCloud de Vários Termos")
plt.show()

#  3- Criar Bag of Words (frequência de palavras)
print('3- Criar Bag of Words (frequência de palavras)')
bow = Counter(lista)
print("Bag of Words:", bow)

#4 - Número total de tokens e diversidade lexica
print('4 - Número total de tokens e diversidade lexica')
numero_tokens = len(docs)
total_tokens_unicos = len(set([t.text for t in docs]))
print(len(docs))
print(total_tokens_unicos)
diversidade_lexica = total_tokens_unicos / numero_tokens
print(diversidade_lexica)

# TTR < 0,3 → texto repetitivo, pouca variedade de vocabulário.
#
# TTR entre 0,3 e 0,6 → vocabulário moderadamente variado.
#
# TTR > 0,6 → texto bastante diverso, muitas palavras diferentes em relação ao tamanho do corpus.

total_pos_taging = Counter()
for doc in docs:
    for token in doc:
        total_pos_taging[token.pos_] += 1

print(total_pos_taging)

print('5-Frequências mais comuns')
freq = Counter([t.text.lower() for t in docs])
print(freq)

# 4 -cluster de tópicos
# print(df_brutos)

print('6-Mostrando palavras-chave de cada cluster')
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(df['valores'])

num_clusters = 3  # você pode ajustar
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
df['cluster'] = kmeans.fit_predict(X)

# Mostrando palavras-chave de cada cluster
terms = vectorizer.get_feature_names_out()
for i in range(num_clusters):
    # pega os índices das linhas que pertencem ao cluster i
    idx = np.where(df['cluster'] == i)[0]
    cluster_terms = X[idx].toarray()  # agora funciona
    mean_tfidf = cluster_terms.mean(axis=0)
    top_terms = [terms[j] for j in mean_tfidf.argsort()[-5:][::-1]]
    print(f"Cluster {i}: {top_terms}")

