import duckdb
from langdetect import detect, DetectorFactory
import spacy

AWS_ACCESS_KEY_ID = "meuusuario"
AWS_SECRET_ACCESS_KEY = "minhasenha123"
AWS_ENDPOINT_URL = "172.40.0.21:9000"  # host:porta
BUCKET_NAME = "meu-bucket"
ARQUIVO_PATH = "datalake/bronze/data_2025_09_01/jogo_244850/reviews.json"
con = duckdb.connect()
con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
con.execute(f"SET s3_endpoint='{AWS_ENDPOINT_URL}';")
con.execute("SET s3_region='us-east-1';")      # DuckDB exige, mesmo que MinIO ignore
con.execute("SET s3_use_ssl=false;")          # usa HTTP
con.execute("SET s3_url_style='path';")       # estilo path obrigatório para MinIO



df = con.execute(f"""
    SELECT * FROM read_json_auto('s3://{BUCKET_NAME}/{ARQUIVO_PATH}')
""").df()
# Detectar textos em português
def is_portuguese(text):
    try:
        return detect(text) == 'pt'
    except:
        return False

df['portugues'] = df['review'].apply(is_portuguese)

# Mostrar apenas reviews em português
print(df[df['portugues'] == True]['review'])
