import duckdb
from langdetect import detect, DetectorFactory
from spacy import displacy
import spacy
import unicodedata
import re
import pathlib

#  read_json_auto
AWS_ACCESS_KEY_ID = "meuusuario"
AWS_SECRET_ACCESS_KEY = "minhasenha123"
AWS_ENDPOINT_URL = "172.40.0.21:9000"  # host:porta
BUCKET_NAME = "meu-bucket"
ARQUIVO_PATH = "datalake/prata/comentarios_brutos/jogo_244850/reviews_bruto.parquet"
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

lista_setenca = df["valores"].dropna().astype(str).tolist()

# Carrega modelo SpaCy
nlp = spacy.load("pt_core_news_sm")

output_dir = pathlib.Path("saida_displacy")
output_dir.mkdir(exist_ok=True)

for i, texto in enumerate(lista_setenca, start=1):
    doc = nlp(texto)
    print(f"\n🔹 Review {i}: {texto}")

    # Exibir dependência sintática dos tokens
    for token in doc:
        print(
            f"token={token.text:<15} "
            f"dep={token.dep_:<10} "
            f"head={token.head.text: <15}  "
            f"pos={token.pos_: <20} "
            f"tag={token.tag_: <20}"
            f"lemma={token.lemma_: <20}"
            f"morph={str(token.morph):<40}"
        )

        for ent in doc.ents:
            print('doc ents' * 20)
            print(ent.text, ent.label_, ent.conjuncts)

    print("=" * 100)
