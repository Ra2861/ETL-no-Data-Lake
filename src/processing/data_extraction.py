import psycopg2
import pandas as pd
import logging
from tqdm import tqdm
import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 15000))  # Dynamically set batch size

def list_supabase_tables(conn):
    """
  Lista as tabelas no banco de dados Supabase.

  Esta função se conecta ao Supabase e executa uma consulta para obter o 
  nome de todas as tabelas no schema 'private_schema'. Ela então registra 
  as tabelas encontradas e retorna a lista de nomes.

  Parâmetros:
      conn (psycopg2.connect): Conexão estabelecida com o banco de dados Supabase.

  Retorna:
      list: Lista de strings contendo os nomes das tabelas encontradas.
  """
    logging.info("Listing tables in Supabase...")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'private_schema'")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        logging.info(f"Found tables in Supabase: {table_names}")
        return table_names
    except psycopg2.Error as e:
        logging.error(f"Error listing tables in Supabase: {e}")
        raise


def extract_data_from_supabase(conn, table_name):
    """
  Extrai dados da tabela Supabase especificada.

  Esta função recebe o nome da tabela e a conexão com o banco de dados. 
  Ela então constrói uma consulta para selecionar todos os dados da tabela 
  indicada. A extração é feita em lotes utilizando `fetchmany` para 
  melhorar a performance. Por fim, a função retorna um gerador que produz 
  DataFrames com os dados extraídos em lotes.

  Parâmetros:
      conn (psycopg2.connect): Conexão estabelecida com o banco de dados Supabase.
      table_name (str): Nome da tabela a ser extraída.

  Retorna:
      generator: Gerador que produz DataFrames contendo os dados da tabela em lotes.
  """
    logging.info(f"Extracting data from table '{table_name}' in Supabase...")
    cursor = conn.cursor()
    query = f"SELECT * FROM private_schema.{table_name}"
    try:
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    except psycopg2.Error as e:
        logging.error(f"Error extracting data from '{table_name}' in Supabase: {e}")
        raise
