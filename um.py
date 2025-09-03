import duckdb
from langdetect import detect, DetectorFactory
import spacy
import unicodedata
import re
#  read_json_auto
AWS_ACCESS_KEY_ID = "meuusuario"
AWS_SECRET_ACCESS_KEY = "minhasenha123"
AWS_ENDPOINT_URL = "172.40.0.21:9000"  # host:porta
BUCKET_NAME = "meu-bucket"
ARQUIVO_PATH = "datalake/prata/data_2025_09_03/jogo_244850/reviews.parquet"
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

# Configura seed do langdetect (para resultados consistentes)
print(df)