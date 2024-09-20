import sys
import os
import logging
from tqdm import tqdm
# Add the top-level 'src' directory to the Python path (assuming project structure)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import necessary modules
from src.utils.connections import connect_to_supabase, connect_to_clickhouse  # Connection functions
from src.utils.clickhouse_utils import (  # ClickHouse utility functions
    ensure_clickhouse_table,
    load_data_to_clickhouse,
    check_existing_records,
)
from src.processing.data_extraction import (  # Data extraction functions
    list_supabase_tables,
    extract_data_from_supabase,
)
from src.processing.data_cleaning import transform_data, remove_duplicates  # Data cleaning functions


def etl_process():
  """
  Executa o processo ETL completo (Extract, Transform, Load).

  1. Conecta-se ao Supabase e ClickHouse.
  2. Verifica se a tabela ClickHouse existe e a cria caso necessário.
  3. Obtém a lista de tabelas do Supabase.
  4. Loop por cada tabela:
      - Verifica se já existem registros na tabela ClickHouse com a tag da tabela atual.
        - Se sim, pula a tabela para evitar duplicatas.
      - Extrai os dados da tabela Supabase em lotes usando um loop tqdm para progresso.
      - Para cada lote:
          - Aplica a transformação de dados usando a função `transform_data`.
          - Remove duplicatas remanescentes usando `remove_duplicates`.
          - Carrega os dados transformados no ClickHouse usando `load_data_to_clickhouse`.
  5. Fecha as conexões com o Supabase e ClickHouse.
  6. Registra a conclusão do processo ETL.
  """
  conn = connect_to_supabase()
  clickhouse_client = connect_to_clickhouse()

  try:
    ensure_clickhouse_table(clickhouse_client)  # Cria tabela ClickHouse se não existir

    tables = list_supabase_tables(conn)  # Lista de tabelas Supabase
    for table_name in tables:
      if check_existing_records(clickhouse_client, 'grupox', table_name):  # Verifica duplicatas
        logging.info(f"Pulando tabela '{table_name}' (registros com tag '{table_name}' já existem).")
        continue  # Pular tabela existente

      for batch_df in tqdm(extract_data_from_supabase(conn, table_name), desc=f"Processando tabela '{table_name}'"):
        transformed_df = transform_data(batch_df, table_name)  # Aplica transformações
        transformed_df = remove_duplicates(transformed_df)  # Remove duplicatas
        load_data_to_clickhouse(clickhouse_client, transformed_df)  # Carrega dados no ClickHouse

    logging.info("Todas as tabelas processadas. Fechando conexões.")
  finally:
    conn.close()
    logging.info("Processo ETL concluído com sucesso.")


if __name__ == "__main__":
  etl_process()