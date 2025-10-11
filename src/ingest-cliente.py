from pyspark.sql import SparkSession

def main():
    """
    Função principal que inicializa o Spark, conecta ao PostgreSQL,
    lê os dados da tabela 'clientes' e encerra a sessão.
    """
    
    # Inicializa a SparkSession.
    # A configuração abaixo instrui o Spark a baixar e gerenciar o driver
    # do PostgreSQL automaticamente.
    spark = (
        SparkSession.builder
        .appName("IngestClientesModerno")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    print("Sessão Spark iniciada com sucesso!")

    # Parâmetros de conexão com o banco de dados
    conn_params = {
        "host": "db",
        "database": "mydb",
        "user": "myuser",
        "password": "mypassword"
    }

    # Monta a URL JDBC e o dicionário de propriedades
    jdbc_url = f"jdbc:postgresql://{conn_params['host']}:5432/{conn_params['database']}"
    connection_properties = {
        "user": conn_params['user'],
        "password": conn_params['password'],
        "driver": "org.postgresql.Driver"
    }
    print(f"URL JDBC construída: {jdbc_url}")

    # Query SQL para ler todos os dados da tabela (use o schema.tabela correto)
    minha_query = "(SELECT * FROM db_loja.clientes) AS todos_clientes"

    print("Executando a leitura da query no banco de dados...")

    try:
        # Lê os dados usando a query e os carrega em um DataFrame
        df_clientes = spark.read.jdbc(
            url=jdbc_url,
            table=minha_query,
            properties=connection_properties
        )

        print("Leitura via query concluída com sucesso!")

        # Exibe os resultados para verificação
        print("\nSchema do DataFrame:")
        df_clientes.printSchema()

        print("\nAmostra dos clientes (10 primeiras linhas):")
        df_clientes.show(10, truncate=False)

        print(f"\nTotal de registros encontrados: {df_clientes.count()}")

    except Exception as e:
        print(f"OCORREU UM ERRO: {e}")
    finally:
        # Garante que a sessão Spark seja sempre encerrada
        print("Encerrando a sessão Spark.")
        spark.stop()

if __name__ == '__main__':
    main()