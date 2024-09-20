import logging
import psycopg2
import clickhouse_connect
import sys
import os

# Adiciona o diretório principal 'src' ao caminho do Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Agora importa de utils e config
from src.utils.retry import retry_on_failure  # Função para reconexão em caso de falha
from src.config.config import (  # Configurações de conexão
    SUPABASE_DB, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_HOST, SUPABASE_PORT,
    TIMEOUT,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
)

# Define o timeout da consulta em milissegundos (aumente se necessário)
TIMEOUT = 60000000000  # 600 segundos

@retry_on_failure
def connect_to_supabase():
  """
  Estabelece conexão com o banco de dados Supabase via PostgreSQL.

  A função utiliza a biblioteca `psycopg2` para conectar ao Supabase. 
  Ela lê as credenciais e parâmetros de conexão (host, porta, database) 
  das variáveis de ambiente definidas no arquivo `config.py`.

  A função também define um timeout para consultas (opcionalmente ajustável 
  através da variável `TIMEOUT`) para evitar travamentos em casos de lentidão.

  Retorna:
      psycopg2.connect: Conexão estabelecida com o banco de dados Supabase.
  """
  logging.info("Conectando ao Supabase PostgreSQL...")
  try:
    conn = psycopg2.connect(
      dbname=SUPABASE_DB,
      user=SUPABASE_USER,
      password=SUPABASE_PASSWORD,
      host=SUPABASE_HOST,
      port=SUPABASE_PORT,
      options=f"-c statement_timeout={TIMEOUT}"
    )
    logging.info("Conexão ao Supabase estabelecida.")
    return conn
  except psycopg2.Error as e:
    logging.error(f"Erro ao conectar ao Supabase: {e}")
    raise

@retry_on_failure
def connect_to_clickhouse():
  """
  Estabelece conexão com o banco de dados ClickHouse.

  A função utiliza a biblioteca `clickhouse_connect` para conectar ao ClickHouse. 
  Ela lê as credenciais e parâmetros de conexão (host, porta, database, usuário, senha) 
  das variáveis de ambiente definidas no arquivo `config.py`.

  A função também configura a conexão como segura (`secure=True`) e desabilita 
  a verificação de certificado (`verify=False`). É importante ressaltar que a 
  verificação de certificado deve ser habilitada em um ambiente de produção.

  Retorna:
      clickhouse_connect.Client: Conexão estabelecida com o banco de dados ClickHouse.
  """
  logging.info("Conectando ao ClickHouse...")
  try:
    client = clickhouse_connect.get_client(
      host=CLICKHOUSE_HOST,
      port=CLICKHOUSE_PORT,
      username=CLICKHOUSE_USER,
      password=CLICKHOUSE_PASSWORD,
      database=CLICKHOUSE_DB,
      secure=True,
      verify=False
    )
    logging.info("Conexão ao ClickHouse estabelecida.")
    return client
  except Exception as e:
    logging.error(f"Erro ao conectar ao ClickHouse: {e}")
    raise