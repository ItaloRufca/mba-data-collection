#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline para silver/dbloja (v6 - Nome de arquivo Silver igual Bronze).
Estratégia sem s3a://:
1. Lista/baixa arquivos da Bronze (dbloja) usando o MinIO SDK.
2. Lê os arquivos Parquet *locais* com Spark (file://), lendo tipos conforme Parquet.
3. Converte colunas Long -> Timestamp e Double -> Decimal.
4. Aplica schemas, limpa e salva o resultado localmente (com nome part-...).
5. Gera nome de arquivo final (nome_tabela_YYYYMMDD_HHMMSS.parquet).
6. Sobe o arquivo local (part-...) para a Silver COM O NOVO NOME usando o MinIO SDK.
"""

import os
import io
import tempfile 
import shutil   
import glob     
from typing import Iterable
# --- ADICIONADO DATETIME ---
from datetime import datetime 

# Imports das bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from minio import Minio
from minio.error import S3Error
from minio.deleteobjects import DeleteObject 

# Imports dos tipos do Spark
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType, DecimalType, 
    BooleanType, LongType 
)

# ---------------- CONFIG ---------------- #
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET           = os.getenv("DL_BUCKET", "datalake")
BRONZE_BASE_PREFIX = "bronze/dbloja/" 
SILVER_BASE_PREFIX = "silver/dbloja/" 
# ---------------------------------------- #

# --- FUNÇÕES HELPER (Sem mudanças) ---
def connect_minio() -> Minio:
    print("Conectando ao MinIO…")
    cli = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    cli.list_buckets()
    print("OK: Conectado ao MinIO.")
    return cli

def download_prefix_to_dir(minio_client: Minio, prefix: str, local_dir: str) -> int:
    print(f"  Baixando arquivos de 's3://{BUCKET}/{prefix}'...")
    num_files = 0
    try:
        objetos = minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
        for obj in objetos:
            if obj.object_name.endswith(".parquet"):
                caminho_relativo = os.path.relpath(obj.object_name, prefix)
                local_file = os.path.join(local_dir, caminho_relativo)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                minio_client.fget_object(BUCKET, obj.object_name, local_file)
                num_files += 1
        if num_files == 0: print(f"  Nenhum arquivo .parquet encontrado em {prefix}. Pulando.")
        else: print(f"  {num_files} arquivos baixados para {local_dir}")
        return num_files
    except Exception as e:
        print(f"  ERRO ao baixar da Bronze: {e}. Pulando tabela.")
        return 0

def remove_prefix(minio_client: Minio, prefix: str):
    print(f"  Limpando diretório Silver antigo: s3://{BUCKET}/{prefix}")
    try:
        to_delete: Iterable[DeleteObject] = (DeleteObject(obj.object_name) for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True))
        errors = list(minio_client.remove_objects(BUCKET, to_delete))
        if errors:
            print("  Falhas ao remover objetos antigos:"); [print(f"  - {e}") for e in errors]
            # Decidimos não levantar erro fatal aqui, apenas avisar
            # raise RuntimeError("Falhas ao remover objetos antigos no Silver.") 
        print("  Diretório Silver limpo.")
    except Exception as e:
        print(f"  Aviso: Não foi possível limpar diretório Silver (pode não existir): {e}")

# --- FUNÇÃO upload_directory REMOVIDA (não precisamos mais dela) ---

# --- PROGRAMA PRINCIPAL ---
def main():
    print("Iniciando clientes Spark e MinIO...")
    spark = None
    workdir = tempfile.mkdtemp(prefix="spark_silver_dbloja_")
    print(f"Usando diretório temporário: {workdir}")

    try:
        # --- ETAPA 2.1: INICIAR SPARK ---
        spark = SparkSession.builder \
            .appName("ProcessamentoSilver_dbloja") \
            .master("local[*]") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Sessão Spark iniciada com sucesso.")

        # --- ETAPA 2.2: INICIAR MINIO ---
        minio = connect_minio()

        # --- ETAPA 3: DEFINIÇÃO DE SCHEMAS (Corrigidos) ---
        schema_cliente = StructType([
            StructField("id", LongType(), True), StructField("nome", StringType(), True),
            StructField("email", StringType(), True), StructField("telefone", StringType(), True),
            StructField("data_cadastro", LongType(), True), StructField("is_date", BooleanType(), True) 
        ])
        schema_categorias_produto = StructType([
            StructField("id", LongType(), True), StructField("nome", StringType(), True),
            StructField("descricao", StringType(), True)
        ])
        schema_pedido_cabecalho = StructType([
            StructField("id", LongType(), True), StructField("id_cliente", LongType(), True), 
            StructField("data_pedido", LongType(), True), StructField("valor_total", DoubleType(), True) 
        ])
        schema_pedido_itens = StructType([
            StructField("id", LongType(), True), StructField("id_pedido", LongType(), True), 
            StructField("id_produto", LongType(), True), StructField("quantidade", LongType(), True), 
            StructField("preco_unitario", DoubleType(), True) 
        ])
        schema_produto = StructType([
            StructField("id", LongType(), True), StructField("nome", StringType(), True), 
            StructField("id_categoria", LongType(), True), StructField("preco", DoubleType(), True), 
            StructField("data_atualizacao", LongType(), True) 
        ])

        schemas = {
            "cliente": schema_cliente, "categorias_produto": schema_categorias_produto,
            "pedido_cabecalho": schema_pedido_cabecalho, "pedido_itens": schema_pedido_itens,
            "produto": schema_produto 
        }
        colunas_timestamp = {
            "cliente": ["data_cadastro"], "pedido_cabecalho": ["data_pedido"],
            "produto": ["data_atualizacao"]
        }
        colunas_decimal = {
            "pedido_cabecalho": {"valor_total": DecimalType(10, 2)},
            "pedido_itens": {"preco_unitario": DecimalType(10, 2)},
            "produto": {"preco": DecimalType(10, 2)}
        }

        tabelas_a_processar = list(schemas.keys()) 
        print(f"\n--- Iniciando Processamento Carga Full (Overwrite) para: {tabelas_a_processar} ---")

        # --- ETAPA 4: LOOP DE PROCESSAMENTO ---
        for nome_tabela in tabelas_a_processar:
            print(f"\nProcessando tabela: {nome_tabela}...")
            
            # 1. Definir prefixos e pastas temporárias
            minio_bronze_prefix = f"{BRONZE_BASE_PREFIX}{nome_tabela}/"
            minio_silver_prefix = f"{SILVER_BASE_PREFIX}{nome_tabela}/" # Ex: silver/dbloja/cliente/
            local_bronze_dir = os.path.join(workdir, "bronze", nome_tabela)
            local_silver_dir = os.path.join(workdir, "silver", nome_tabela)

            # 2. Download da Bronze
            arquivos_baixados = download_prefix_to_dir(minio, minio_bronze_prefix, local_bronze_dir)
            if arquivos_baixados == 0: continue 

            # 3. Processamento (Spark Local)
            schema_definido = schemas.get(nome_tabela)
            if not schema_definido: print(f"  AVISO: Schema não definido para '{nome_tabela}'. Pulando."); continue

            print(f"  Processando dados com Spark lendo de: file://{local_bronze_dir}")
            df_bronze = spark.read.schema(schema_definido).parquet(f"file://{local_bronze_dir}")

            # 3.1: Conversão Long -> Timestamp 
            if nome_tabela in colunas_timestamp:
                cols_para_converter = colunas_timestamp[nome_tabela]
                print(f"  Convertendo colunas Long para Timestamp: {cols_para_converter}")
                for col_nome in cols_para_converter:
                    df_bronze = df_bronze.withColumn(col_nome, F.timestamp_micros(F.col(col_nome)))
            
            # 3.2: Conversão Double -> Decimal
            if nome_tabela in colunas_decimal:
                mapa_conversao = colunas_decimal[nome_tabela]
                print(f"  Convertendo colunas Double para Decimal: {list(mapa_conversao.keys())}")
                for col_nome, tipo_decimal in mapa_conversao.items():
                    df_bronze = df_bronze.withColumn(col_nome, F.col(col_nome).cast(tipo_decimal))

            # 4. Transformação (Coalesce)
            df_silver = df_bronze.coalesce(1)
            
            # 5. Salvar Resultado (Spark salva localmente)
            print(f"  Spark salvando resultado em: file://{local_silver_dir}")
            print("  Schema final antes de salvar:")
            df_silver.printSchema() 
            
            df_silver.write.mode("overwrite").parquet(f"file://{local_silver_dir}")
                
            # 6. Upload para a Silver (MinIO SDK)
            
            # 6.1: Encontrar o arquivo 'part-...' gerado pelo Spark
            resultado_files = glob.glob(f"{local_silver_dir}/part-*.parquet")
            if not resultado_files: print("  ERRO: Spark não produziu arquivo de resultado. Pulando upload."); continue
            arquivo_resultado_local = resultado_files[0] # Pegamos o único arquivo
            
            # --- INÍCIO DA MUDANÇA (Nome do Arquivo) ---
            # 6.2: Gerar o nome do arquivo final no formato da Bronze
            now = datetime.now()
            file_timestamp_str = now.strftime("%Y%m%d_%H%M%S") 
            silver_file_name = f"{nome_tabela}_{file_timestamp_str}.parquet"
            
            # 6.3: Montar o caminho completo do objeto no MinIO
            # Ex: silver/dbloja/cliente/cliente_20251025_193000.parquet
            caminho_silver_minio_obj = f"{minio_silver_prefix}{silver_file_name}" 
            # --- FIM DA MUDANÇA ---
            
            # 6.4: Limpar o diretório Silver antigo (Overwrite)
            remove_prefix(minio, minio_silver_prefix)
            
            # 6.5: Fazer upload do arquivo local (part-...) com o NOVO nome
            print(f"  Fazendo upload de '{arquivo_resultado_local}' para '{caminho_silver_minio_obj}'")
            minio.fput_object(
                BUCKET,
                caminho_silver_minio_obj, # <-- Nome do objeto no MinIO
                arquivo_resultado_local,  # <-- Caminho do arquivo local (part-...)
                content_type='application/octet-stream'
            )
            print(f"  Sucesso! Tabela '{nome_tabela}' salva em s3://{BUCKET}/{minio_silver_prefix}")

        print("\n--- Processamento Carga Full concluído ---")

    except Exception as e:
        print(f"\n--- ERRO DURANTE O PROCESSAMENTO ---")
        import traceback
        traceback.print_exc() 

    finally:
        # --- ETAPA 5: ENCERRAR E LIMPAR ---
        if spark:
            spark.stop()
            print("\nSessão Spark encerrada.")
            
        if os.path.exists(workdir):
            try:
                shutil.rmtree(workdir)
                print(f"Diretório temporário {workdir} removido.")
            except Exception as e:
                print(f"AVISO: Não foi possível remover o diretório temporário {workdir}. Erro: {e}")

if __name__ == "__main__":
    main()