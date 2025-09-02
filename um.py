import duckdb
from langdetect import detect, DetectorFactory
import spacy
import unicodedata
import re
from pycparser.ply.ctokens import t_XOR

AWS_ACCESS_KEY_ID = "meuusuario"
AWS_SECRET_ACCESS_KEY = "minhasenha123"
AWS_ENDPOINT_URL = "172.40.0.21:9000"  # host:porta
BUCKET_NAME = "meu-bucket"
ARQUIVO_PATH = "datalake/bronze/data_2025_09_01/jogo_275850/reviews.json"
con = duckdb.connect('dados_reviews/reviews.duckdb')
con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
con.execute(f"SET s3_endpoint='{AWS_ENDPOINT_URL}';")
con.execute("SET s3_region='us-east-1';")  # DuckDB exige, mesmo que MinIO ignore
con.execute("SET s3_use_ssl=false;")  # usa HTTP
con.execute("SET s3_url_style='path';")  # estilo path obrigatório para MinIO

df = con.execute(f"""
    SELECT * FROM read_json_auto('s3://{BUCKET_NAME}/{ARQUIVO_PATH}')
""").df()


def remover_acentos(texto):
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    return ''.join([c for c in texto_normalizado if not unicodedata.combining(c)])


def is_portuguese(text):
    try:
        return detect(text) == 'pt'
    except:
        return False


df['portugues'] = df['review'].apply(is_portuguese)
df_pt = df[df['portugues']]

tupla_de_linhas = df_pt['review'].tolist()
texto_completo = ' '.join(tupla_de_linhas)





emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # símbolos e pictogramas
                           u"\U0001F680-\U0001F6FF"  # transporte e símbolos
                           u"\U0001F1E0-\U0001F1FF"  # bandeiras
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)

def remover_acentos(texto):
    # Normaliza e remove acentos
    return ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )

def fazer_preprocessamento(texto):
    texto = re.sub(r'\s+', ' ', texto)
    texto = emoji_pattern.sub(r'', texto)  # remove emojis
    doc = nlp(texto)

    tokens_processados = set()

    for token in doc:
        if not token.is_stop and not token.is_punct and not token.is_digit and not token.is_space:
            token_sem_acento = remover_acentos(token.lemma_.lower())
            tokens_processados.add(token_sem_acento)

    return tokens_processados


# Camada prata,
# Quando existir, consultar na camada bronze, fazer processamento e gravar na camada prata o tratamento e gravar como parquet

tokens = fazer_preprocessamento(texto_completo)
print(set(tokens))


import pickle

# Supondo que tokens seja o set que você gerou
# tokens = fazer_preprocessamento(texto_completo)

# Método 1: usando pickle para estimar tamanho em memória
tamanho_bytes = len(pickle.dumps(tokens))
tamanho_mb = tamanho_bytes / (1024 * 1024)

print(f"Tamanho estimado do set de tokens: {tamanho_mb:.4f} MB")

import sys


def tamanho_preciso(obj):
    """Calcula o tamanho aproximado de um objeto e seus elementos recursivamente"""
    tamanho_total = sys.getsizeof(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            tamanho_total += tamanho_preciso(k)
            tamanho_total += tamanho_preciso(v)
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            tamanho_total += tamanho_preciso(item)
    elif isinstance(obj, str):
        tamanho_total += sys.getsizeof(obj)

    return tamanho_total


# Supondo que tokens seja o set
tamanho_bytes = tamanho_preciso(tokens)
tamanho_mb = tamanho_bytes / (1024 * 1024)

print(f"Tamanho real aproximado do set de tokens: {tamanho_mb:.4f} MB")