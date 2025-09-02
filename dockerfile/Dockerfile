FROM apache/airflow:3.0.4-python3.11

# Troca para o usuário airflow
USER airflow

# Instala spaCy e o modelo
RUN pip install --no-cache-dir spacy==3.6.1 \
    && python -m spacy download pt_core_news_sm
