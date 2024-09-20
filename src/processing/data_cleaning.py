import logging
import pandas as pd
from tqdm import tqdm
import json


def clean_data(df):
    """
        Limpa o DataFrame removendo valores nulos.

        Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

        Parâmetros:
            df (pd.DataFrame): DataFrame a ser limpo.

        Retorna:
        pd.DataFrame: DataFrame limpo sem valores nulos.
    """
    logging.info("Cleaning data...")
    with tqdm(total=len(df), desc="Cleaning data") as pbar:
        df.dropna(inplace=True)
        pbar.update(len(df))
    return df


def normalize_data(df):
    """
        Normaliza o DataFrame aplicando formatação padrão.

        * Converte strings para minúsculo.
        * Formata colunas de data e hora (se houver) para o formato "%Y-%m-%d %H:%M:%S".

         Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

        Parâmetros:
         df (pd.DataFrame): DataFrame a ser normalizado.

        Retorna:
      pd.DataFrame: DataFrame normalizado.
    """
    logging.info("Normalizing data...")
    with tqdm(total=len(df), desc="Normalizing data") as pbar:
        df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
        for col in df.select_dtypes(include=['datetime']):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        pbar.update(len(df))
    return df


def aggregate_data(df):
    """
     Agrega dados se necessário.

        * Agrupa por 'category' e soma a coluna 'amount' (se presentes).

        Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

        Parâmetros:
      df (pd.DataFrame): DataFrame a ser agregado.

        Retorna:
      pd.DataFrame: DataFrame agregado (se aplicável).
    """
     
    logging.info("Aggregating data...")
    with tqdm(total=len(df), desc="Aggregating data") as pbar:
        if 'category' in df.columns and 'amount' in df.columns:
            df = df.groupby('category', as_index=False)['amount'].sum()
        pbar.update(len(df))
    return df


def filter_data(df):
    """
  Filtra dados com base em condições estabelecidas.

  * Filtra por registros com status 'active' (se a coluna 'status' existir).

  Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser filtrado.

  Retorna:
      pd.DataFrame: DataFrame filtrado (se aplicável).
  """
    logging.info("Filtering data...")
    with tqdm(total=len(df), desc="Filtering data") as pbar:
        if 'status' in df.columns:
            df = df[df['status'] == 'active']
        pbar.update(len(df))
    return df


def convert_data_types(df):
    """
  Converte tipos de dados conforme necessário.

  * Tenta converter colunas do tipo 'object' para numérico.
  * Converte colunas com 'date' no nome para datetime (tratando erros).

  Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser convertido.

  Retorna:
      pd.DataFrame: DataFrame com tipos de dados convertidos.
  """
    logging.info("Converting data types...")
    with tqdm(total=len(df), desc="Converting data types") as pbar:
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col])
                except ValueError:
                    continue
        for col in df.columns:
            if 'date' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        pbar.update(len(df))
    return df


def remove_duplicates(df):
    """
    Remove registros duplicados do DataFrame.

  Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser processado.

  Retorna:
      pd.DataFrame: DataFrame sem registros duplicados.
  """
    logging.info("Removing duplicates...")
    with tqdm(total=len(df), desc="Removing duplicates") as pbar:
        df.drop_duplicates(inplace=True)
        pbar.update(len(df))
    return df




def map_fields(df):
    """
  Remapea nomes de colunas de acordo com um mapeamento especificado.

  * O mapeamento deve ser um dicionário onde a chave é o nome antigo da coluna 
    e o valor é o novo nome desejado.

  Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser processado.

  Retorna:
      pd.DataFrame: DataFrame com colunas renomeadas.
  """
    logging.info("Mapping fields...")
    with tqdm(total=len(df), desc="Mapping fields") as pbar:
        column_mapping = {'old_column_name': 'new_column_name'}
        df.rename(columns=column_mapping, inplace=True)
        pbar.update(len(df))
    return df


def validate_data(df):
    """
  Valida os dados de acordo com critérios específicos.

  * Neste exemplo, valida a coluna 'amount' para estar entre 0 e 1.000.000.

  Utiliza a barra de progresso `tqdm` para acompanhar o processamento.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser processado.

  Retorna:
      pd.DataFrame: DataFrame contendo apenas registros válidos (se aplicável).
  """
    logging.info("Validating data...")
    with tqdm(total=len(df), desc="Validating data") as pbar:
        if 'amount' in df.columns:
            df = df[df['amount'].between(0, 1000000)]
        pbar.update(len(df))
    return df


def transform_data(df, table_name):  
    """
  Aplica todas as transformações no DataFrame e adiciona metadados.

  Esta função aplica sequencialmente todas as transformações de dados 
  definidas anteriormente, incluindo:

      * Limpeza (remoção de valores nulos)
      * Normalização (conversão para minúsculo, formatação de data e hora)
      * Agregação (se aplicável, por categoria e soma de valores)
      * Filtragem (se aplicável, por status ativo)
      * Conversão de tipos de dados (tentativa para numérico e datetime)
      * Remoção de duplicatas
      * Mapeamento de nomes de colunas (conforme mapeamento especificado)
      * Validação (se aplicável, por faixa de valores)

  Além disso, a função adiciona duas novas colunas ao DataFrame:

      * 'data': Uma string JSON contendo a representação do DataFrame como dicionário.
      * 'tag': O nome da tabela fornecido como argumento.

  Se a coluna 'date_time' não existir, ela é criada com a data e hora atual.

  Por fim, a função seleciona apenas as colunas 'data', 'date_time' e 'tag' 
  e retorna o DataFrame transformado.

  Parâmetros:
      df (pd.DataFrame): DataFrame a ser processado.
      table_name (str): Nome da tabela associada ao DataFrame.

  Retorna:
      pd.DataFrame: DataFrame transformado e com metadados adicionados.
  """

    logging.info("Starting data transformation...")
    df = clean_data(df)
    df = normalize_data(df)
    df = aggregate_data(df)
    df = filter_data(df)
    df = convert_data_types(df)
    df = remove_duplicates(df)
    df = map_fields(df)
    df = validate_data(df)

    df['data'] = df.apply(lambda x: json.dumps(x.to_dict()).replace("'", "\\'"), axis=1)
    df['tag'] = table_name

    if 'date_time' not in df.columns:
        df['date_time'] = pd.to_datetime('now')

    df = df[['data', 'date_time', 'tag']]
    logging.info("Data transformation completed.")
    return df
