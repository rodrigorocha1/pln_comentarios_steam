import requests

# Endpoint de health do MinIO
minio_endpoint = "http://172.40.0.22:9000/minio/health/ready"

try:
    response = requests.get(minio_endpoint, timeout=5)
    if response.status_code == 200:
        print("MinIO está online!")
    else:
        print(f"MinIO respondeu, mas não está pronto (status {response.status_code})")
except requests.exceptions.RequestException as e:
    print("Não foi possível conectar ao MinIO:", e)