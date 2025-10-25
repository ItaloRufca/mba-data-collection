# -*- coding: utf-8 -*-
import psycopg2
from minio import Minio
from io import BytesIO
from minio.error import S3Error
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import json

JSON_CONTROL_FILE = "bronze/dbloja/data_atualizacao.json"


# --- FUNÇÃO 1: SETUP DO MINIO (Sem alterações) ---
def setup_minio(minio_client, bucket_name, folder_path):
    """Verifica e cria bucket e pastas raiz."""
    try:
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            print(f"Bucket '{bucket_name}' não encontrado. Criando...")
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            print(f"Bucket '{bucket_name}' já existe.")

        for folder in folder_path:
            try:
                minio_client.stat_object(bucket_name, folder)
                print(f"Pasta '{folder}' já existe em '{bucket_name}'.")
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    print(f"Pasta '{folder}' não encontrada. Criando...")
                    minio_client.put_object(
                        bucket_name=bucket_name,
                        object_name=folder,
                        data=BytesIO(b""), 
                        length=0 
                    )
                    print(f"Pasta '{folder}' criada com sucesso.")
                else:
                    raise
        print("\n--- Setup do MinIO concluído ---")
    except Exception as e:
        print(f"\nOcorreu um erro geral durante as operações do Minio: {e}")
        sys.exit(1)


# --- NOVA FUNÇÃO 2: LER TIMESTAMP DE CONTROLE ---
def get_last_run_timestamp(minio_client, bucket_name):
    """
    Busca o timestamp (em formato string ISO) da última execução no MinIO.
    Retorna None se o arquivo não existir (primeira execução).
    """
    try:
        # Tenta baixar o objeto JSON
        response = minio_client.get_object(bucket_name, JSON_CONTROL_FILE)
        # Lê os dados
        data = response.read()
        # Fecha a conexão para liberar recursos
        response.close()
        response.release_conn()
        
        control_data = json.loads(data)
        last_run = control_data.get('last_run')
        
        if last_run:
            print(f"Timestamp da última execução (marca d'água) encontrado: {last_run}")
            return last_run
        else:
            print("Arquivo JSON de controle encontrado, mas sem a chave 'last_run'. Carga total.")
            return None
            
    except S3Error as e:
        if e.code == 'NoSuchKey':
            # Isso é esperado na primeira execução
            print(f"Arquivo '{JSON_CONTROL_FILE}' não encontrado. Executando carga total.")
            return None
    except Exception as e:
        # Trata outros erros (ex: JSON mal formatado)
        print(f"Erro ao ler JSON de controle '{JSON_CONTROL_FILE}' (carga total será executada): {e}")
        return None

# --- NOVA FUNÇÃO 3: ATUALIZAR TIMESTAMP DE CONTROLE ---
def update_last_run_timestamp(minio_client, bucket_name, current_timestamp_iso):
    """
    Salva/Sobrescreve o JSON de controle no MinIO com o timestamp atual (string ISO).
    """
    try:
        # Cria o dicionário python
        control_data = {'last_run': current_timestamp_iso}
        # Converte para bytes JSON
        json_bytes = json.dumps(control_data, indent=2).encode('utf-8')
        json_buffer = BytesIO(json_bytes)
        
        # Faz o upload para o MinIO
        minio_client.put_object(
            bucket_name,
            JSON_CONTROL_FILE,
            data=json_buffer,
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"\nArquivo de controle '{JSON_CONTROL_FILE}' atualizado para: {current_timestamp_iso}")
    except Exception as e:
        print(f"\nERRO CRÍTICO: Não foi possível salvar o timestamp da execução: {e}")
        print("A próxima execução poderá processar dados duplicados.")


# --- FUNÇÃO 4: LÓGICA DE ETL (MODIFICADA) ---
def extract_postgres_to_minio_partitioned(engine, minio_client, bucket_name, bronze_base_path, db_name_path, db_schema):
    """
    Extrai tabelas do Postgres e salva em MinIO com partição de data
    e lógica incremental para a tabela 'produto'.
    
    Salva em um formato compatível com Spark:
    [bronze_base_path]/[nome_tabela]/data=YYYYMMDD/[nome_tabela]_[timestamp].parquet
    """
    print(f"\nIniciando ingestão particionada do schema '{db_schema}'...")

    # --- INÍCIO DA LÓGICA INCREMENTAL ---
    
    # 1. Define o carimbo de data/hora ATUAL. 
    current_run_iso = datetime.now().isoformat()
    
    # 2. Busca o carimbo de data/hora da ÚLTIMA execução (lendo o JSON)
    last_run_iso = get_last_run_timestamp(minio_client, bucket_name)
    
    # !!! ATENÇÃO: MUDE AQUI SE NECESSÁRIO !!!
    # Nome da coluna na tabela 'produto' que armazena a data de modificação.
    NOME_COLUNA_INCREMENTAL = "data_atualizacao" 
    
    # --- FIM DA LÓGICA INCREMENTAL ---

    try:
        with engine.connect() as conn:
            
            query_lista_tabelas = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{db_schema}' AND table_type = 'BASE TABLE';
            """
            
            resultado = conn.execute(text(query_lista_tabelas))
            tabelas = [row[0] for row in resultado]
            
            if not tabelas:
                print(f"Nenhuma tabela encontrada no schema '{db_schema}'.")
                return
            print(f"Tabelas encontradas: {tabelas}")

            # Loop para processar cada tabela
            for nome_tabela in tabelas:
                print(f"Processando tabela: {nome_tabela}...")
                
                query_tabela_str = ""
                
                # --- LÓGICA DO WHERE DINÂMICO ---
                if nome_tabela == 'produto' and last_run_iso is not None:
                    print(f"  -> Carga incremental detectada para 'produto'.")
                    print(f"     Carregando dados DEPOIS DE '{last_run_iso}' ATÉ '{current_run_iso}'")
                    
                    query_tabela_str = f"""
                        SELECT * FROM {db_schema}.{nome_tabela}
                        WHERE {NOME_COLUNA_INCREMENTAL} > '{last_run_iso}'
                          AND {NOME_COLUNA_INCREMENTAL} <= '{current_run_iso}'
                    """
                
                else:
                    if nome_tabela == 'produto':
                        print("  -> Carga total (Full Load) para 'produto'.")
                    
                    query_tabela_str = f"SELECT * FROM {db_schema}.{nome_tabela}"
                # --- FIM DO WHERE ---
                
                # 1. Extração (Extract) com Pandas
                df = pd.read_sql(text(query_tabela_str), conn)
                
                if df.empty:
                    print(f"  A tabela '{nome_tabela}' está vazia ou não há dados novos. Pulando.")
                    continue

                # --- INÍCIO DA MUDANÇA (CAMINHO E NOME DE ARQUIVO) ---

                # 2. Gerar Timestamps e Paths
                now_parquet = datetime.now()
                # Pasta de partição (ex: data=20251025)
                date_partition_str = now_parquet.strftime("data=%Y%m%d")
                
                # Formato: YYYYMMDD_HHMMSS (ex: 20251025_151900)
                file_timestamp_str = now_parquet.strftime("%Y%m%d_%H%M%S") 
                
                # --- AJUSTE PRINCIPAL NO NOME DO ARQUIVO ---
                # Nome do arquivo exatamente como você pediu:
                # Ex: produto_20251025_151900.parquet
                file_name = f"{nome_tabela}_{file_timestamp_str}.parquet"
                
                # --- AJUSTE PRINCIPAL NO CAMINHO ---
                # Adicionamos {nome_tabela} ao caminho
                # Ex: bronze/dbloja/produto/data=20251025/produto_20251025_151900.parquet
                object_name = f"{bronze_base_path}{nome_tabela}/{date_partition_str}/{file_name}"
                
                # --- FIM DA MUDANÇA ---
                
                # 3. Transformação (Converte para Parquet)
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False, compression='snappy', engine='pyarrow')
                data_length = parquet_buffer.tell()
                parquet_buffer.seek(0)
                
                # 4. Carga (Load) - Salva no MinIO
                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=parquet_buffer,
                    length=data_length,
                    content_type='application/octet-stream'
                )
                print(f"  Sucesso! Tabela salva em: '{object_name}' ({len(df)} linhas)")

        # --- SUCESSO! ---
        # 5. Se o loop de todas as tabelas terminou sem erro,
        # atualiza o JSON de controle no MinIO para a *próxima* execução.
        update_last_run_timestamp(minio_client, bucket_name, current_run_iso)
        
        print("\nProcesso de ingestão particionada concluído.")

    except Exception as e:
        print(f"\nOcorreu um erro durante a ingestão do PostgreSQL: {e}")
        print("ATENÇÃO: O timestamp de controle *não* foi atualizado devido ao erro.")

# --- FUNÇÃO PRINCIPAL: ORQUESTRAÇÃO (Sem alterações) ---
def main():
    """Função principal que executa a conexão e o ETL."""

    # --- 1. CONFIGURAÇÕES ---
    MINIO_ENDPOINT = "minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    
    BUCKET_NAME = "datalake"
    FOLDERS_TO_CREATE = [
        "bronze/dbloja/", "silver/dbloja/",
        "bronze/json/", "silver/json/",
        "bronze/ibge/", "silver/ibge/"
    ]
    
    POSTGRES_HOST = "db"
    POSTGRES_DB = "mydb"       
    POSTGRES_USER = "myuser"
    POSTGRES_PASS = "mypassword"
    POSTGRES_PORT = "5432"
    # IMPORTANTE: Verifique se o schema está correto! (db_loja com underscore)
    POSTGRES_SCHEMA = "db_loja" 
    
    DB_NAME_FOR_PATH = "dbloja"
    BRONZE_DEST_PATH = f"bronze/{DB_NAME_FOR_PATH}/" 

    # --- 2. CONEXÃO MINIO E SETUP ---
    print("Tentando se conectar ao servidor Minio...")
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        minio_client.list_buckets()
        print("Conexão com o Minio estabelecida com sucesso!")
        setup_minio(minio_client, BUCKET_NAME, FOLDERS_TO_CREATE)
    except Exception as e:
        print(f"\nOcorreu um erro ao conectar ou configurar o Minio: {e}")
        return

    # --- 3. CONEXÃO POSTGRES E EXECUÇÃO DO ETL ---
    print("\nTentando se conectar ao banco de dados PostgreSQL...")
    try:
        db_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_string)
        
        with engine.connect() as conn:
            print("Conexão com PostgreSQL estabelecida com sucesso!")
            
        extract_postgres_to_minio_partitioned(
            engine=engine,
            minio_client=minio_client,
            bucket_name=BUCKET_NAME,
            bronze_base_path=BRONZE_DEST_PATH,
            db_name_path=DB_NAME_FOR_PATH,
            db_schema=POSTGRES_SCHEMA
        )
    except Exception as e:
        print(f"\nOcorreu um erro ao conectar ou interagir com o PostgreSQL: {e}")
    finally:
        print("\nScript de ingestão finalizado.")

if __name__ == '__main__':
    main()