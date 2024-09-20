import psycopg2
import psycopg2.extras
import os
import logging
from tqdm import tqdm
import sys
import os

# Importa o decorador para reexecução em caso de falha
from src.utils.retry import retry_on_failure


# Adiciona o diretório principal 'src' ao caminho do Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Importa as variáveis de configuração necessárias
from src.config.config import (
    SUPABASE_DB,
    SUPABASE_USER,
    SUPABASE_PASSWORD,
    SUPABASE_HOST,
    SUPABASE_PORT,
    PROCESSED_DATA_DIR,
)


@retry_on_failure
def store_in_supabase(df, table_name, batch_size=25000):
    """
  Armazena dados processados em tabelas Supabase no formato Parquet.

  Esta função recebe um DataFrame `df`, o nome da tabela Supabase (`table_name`)
  e o tamanho do batch (`batch_size`) para inserção em lotes.

  1. **Gera arquivo Parquet:**
      - A função primeiro salva o DataFrame como um arquivo Parquet no diretório 
        `PROCESSED_DATA_DIR` (definido na configuração).

  2. **Estabelece conexão com Supabase:**
      - A função utiliza o decorador `@retry_on_failure` para tentar a conexão 
        com o Supabase em caso de falhas.
      - Ela se conecta ao banco de dados Supabase usando credenciais 
        (armazenadas nas variáveis de ambiente) e define um cursor para executar 
        consultas.

  3. **Cria tabela (se necessário):**
      - A função verifica se a tabela `table_name` já existe no schema 
        `private_schema` do Supabase.
      - Se a tabela não existir, ela cria a tabela usando uma consulta SQL, 
        definindo todas as colunas do DataFrame como texto (TEXT).

  4. **Insere dados em lotes:**
      - A função itera sobre o DataFrame em lotes usando `tqdm` para exibir 
        o progresso do carregamento.
      - Para cada lote, a função converte o DataFrame em uma lista de listas 
        (`values`) e executa uma consulta `INSERT` em lote usando 
        `psycopg2.extras.execute_values` para eficiência.

  5. **Fecha conexão e confirma alterações:**
      - Após a inserção de todos os lotes, a função confirma as alterações 
        no banco de dados (`conn.commit()`) e fecha a conexão e o cursor.
      - O bloco `finally` garante o fechamento da conexão e do cursor, 
        mesmo em caso de exceções.

  Parâmetros:
      df (pandas.DataFrame): DataFrame contendo os dados a serem armazenados.
      table_name (str): Nome da tabela Supabase para armazenamento.
      batch_size (int, opcional): Tamanho do lote para inserção. Padrão: 25000.

  Retorna:
      None
  """
    parquet_path = os.path.join(PROCESSED_DATA_DIR, f"{table_name}.parquet")
    df.to_parquet(parquet_path)

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD,
            host=SUPABASE_HOST,
            port=SUPABASE_PORT
        )
        cursor = conn.cursor()
        schema_name = "private_schema"
        columns = ', '.join([f"{col} TEXT" for col in df.columns])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns});"
        cursor.execute(create_table_query)

        for i in tqdm(range(0, len(df), batch_size), desc=f"Upload {table_name} para Supabase"):
            batch_df = df.iloc[i:i + batch_size]
            values = batch_df.values.tolist()
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES %s"
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=batch_size)

        conn.commit()
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        if conn:
            conn.rollback()
        logging.error(f"Erro ao armazenar dados no Supabase: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

@retry_on_failure
def test_supabase_connection():
    """Testa a conexão com o Supabase."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD,
            host=SUPABASE_HOST,
            port=SUPABASE_PORT
        )
        logging.info("Conexão bem-sucedida com o Supabase PostgreSQL.")
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        logging.error(f"Falha na conexão com o Supabase PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()
