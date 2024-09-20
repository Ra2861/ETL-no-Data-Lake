import logging
from tqdm import tqdm

def ensure_clickhouse_table(client):
  """
  Verifica e cria a tabela ClickHouse 'grupox' se necessário.

  Esta função verifica se a tabela 'grupox' já existe no banco de dados 
  ClickHouse utilizando a consulta `CREATE TABLE IF NOT EXISTS`. Caso a tabela 
  não exista, a consulta a cria com o schema especificado:

      * data: String (armazena os dados do DataFrame transformado como JSON)
      * date_time: DateTime (armazena a data e hora da transformação)
      * tag: String (armazena a tag da tabela Supabase correspondente)

  A tabela utiliza o motor 'MergeTree' para armazenamento otimizado e 
  possui um índice na coluna 'date_time' para facilitar consultas baseadas em data.

  Parâmetros:
      client (clickhouse_driver.Client): Cliente para comunicação com o ClickHouse.

  Retorna:
      None
  """
  create_table_query = """
  CREATE TABLE IF NOT EXISTS grupox (
      data String,
      date_time DateTime,
      tag String
  ) ENGINE = MergeTree()
  ORDER BY date_time;
  """
  try:
    client.command(create_table_query)
    logging.info("Ensured ClickHouse table 'grupox' exists with correct schema.")
  except Exception as e:
    logging.error(f"Error ensuring table in ClickHouse: {e}")
    raise


def check_existing_records(client, table_name, tag):
  """
  Verifica se registros com a tag especificada já existem na tabela.

  Esta função verifica se há registros na tabela `table_name` do ClickHouse 
  que possuem a tag `tag`. Ela executa a seguinte consulta para contabilizar 
  os registros:

      SELECT COUNT(*) FROM {table_name} WHERE tag = '{tag}'

  A função então retorna `True` se a contagem for maior que zero, indicando 
  que existem registros com a tag, ou `False` caso contrário.

  Parâmetros:
      client (clickhouse_driver.Client): Cliente para comunicação com o ClickHouse.
      table_name (str): Nome da tabela ClickHouse a ser verificada.
      tag (str): Tag a ser procurada nos registros.

  Retorna:
      bool: True se registros com a tag existem, False caso contrário.
  """
  logging.info(f"Checking if records with tag '{tag}' already exist in '{table_name}'...")
  query = f"SELECT COUNT(*) FROM {table_name} WHERE tag = '{tag}'"
  result = client.command(query)
  return result > 0  # If records exist, return True


def load_data_to_ClickHouse(client, df):
  """
  Carrega os dados transformados para a tabela ClickHouse 'grupox'.

  Esta função converte o DataFrame `df` em uma lista de tuplas e constrói uma 
  consulta `INSERT` para carregar os dados na tabela 'grupox' do ClickHouse. 
  Ela utiliza um loop `tqdm` para exibir o progresso do carregamento.

  Parâmetros:
      client (clickhouse_driver.Client): Cliente para comunicação com o ClickHouse.
      df (pd.DataFrame): DataFrame contendo os dados a serem carregados.

  Retorna:
      None
  """
  logging.info("Loading data into ClickHouse...")
  try:
    data_tuples = list(df.itertuples(index=False, name=None))
    insert_query = """
    INSERT INTO grupox (data, date_time, tag)
    VALUES
    """
    values = ', '.join(
        f"('{data}', '{date_time}', '{tag}')" for (data, date_time, tag) in data_tuples
    )
    full_query = f"{insert_query} {values};"
    with tqdm(total=len(df), desc="Loading data to ClickHouse") as pbar:
      client.command(full_query)
      pbar.update(len(df))
    logging.info("Data loaded into ClickHouse.")
  except Exception as e:
    logging.error(f"Error loading data into ClickHouse: {e}")
    raise