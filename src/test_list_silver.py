from minio import Minio
import pandas as pd
from io import BytesIO
import os

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET = os.getenv("DL_BUCKET", "datalake")
SILVER_PREFIX = "silver/dbloja/"

def view_silver_table(table_name: str):
    """Faz download e exibe no terminal o conteúdo de um arquivo Parquet da camada Silver."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        print(f"Conectado ao MinIO ({MINIO_ENDPOINT})")
        prefix = f"{SILVER_PREFIX}{table_name}/"

        parquet_files = [
            obj for obj in client.list_objects(BUCKET, prefix=prefix, recursive=True)
            if obj.object_name.endswith('.parquet')
        ]

        if not parquet_files:
            print(f"Nenhum arquivo encontrado para '{table_name}' em {prefix}")
            return

        # Seleciona o arquivo mais recente (último nomeado por timestamp)
        parquet_files.sort(key=lambda x: x.object_name, reverse=True)
        latest_file = parquet_files[0]

        print(f"Baixando '{latest_file.object_name}'...\n")
        response = client.get_object(BUCKET, latest_file.object_name)
        data = response.read()
        response.close()
        response.release_conn()

        # Lê o arquivo Parquet diretamente da memória
        df = pd.read_parquet(BytesIO(data))

        print(f"\nPrévia dos dados da tabela '{table_name}':\n")
        print(df.head(20).to_string(index=False))
        print(f"\nTotal de linhas: {len(df)}")

    except Exception as e:
        print(f"Erro ao ler tabela '{table_name}': {e}")

if __name__ == "__main__":
    nome_tabela = input("Digite o nome da tabela (ex: produto, cliente, pedido_itens): ").strip()
    view_silver_table(nome_tabela)
