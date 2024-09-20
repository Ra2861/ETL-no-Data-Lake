

# Documentação do Processo ETL

## Estrutura do Projeto

Aqui estão os principais arquivos desse projeto:

- **`data_cleaning.py`**: Cuida da limpeza e transformação dos dados, como remover nulos, normalizar, etc.
- **`data_extraction.py`**: Extrai os dados de arquivos CSV ou JSON para preparar o próximo passo.
- **`etl_process.py`**: Junta tudo, faz o processo ETL completo e carrega os dados limpos para ClickHouse e Supabase.
- **`clickhouse_utils.py`**: Funções para conectar e salvar dados no ClickHouse.
- **`supabase_utils.py`**: Funções para salvar dados no Supabase.
- **`connections.py`**: Gerencia as conexões com o banco, incluindo tentativas de reconexão se der erro.
- **`retry.py`**: Implementa a lógica de repetição para casos de falhas momentâneas.

## Requisitos



```bash
pip install -r requirements.txt
```

## Configuração

 `.env` ou exportar no terminal. Exemplo:

```bash
SUPABASE_DB=<seu_banco_supabase>
SUPABASE_USER=<seu_usuario_supabase>
SUPABASE_PASSWORD=<sua_senha_supabase>
SUPABASE_HOST=<seu_host_supabase>
SUPABASE_PORT=<sua_porta_supabase>

CLICKHOUSE_DB=<seu_banco_clickhouse>
CLICKHOUSE_HOST=<seu_host_clickhouse>
CLICKHOUSE_PORT=<sua_porta_clickhouse>
```

## O Processo ETL

### 1. Extração dos Dados (`data_extraction.py`)

Esse módulo cuida da extração dos dados. Ele carrega os dados de arquivos CSV e JSON e os joga em DataFrames do Pandas para facilitar a manipulação.

- **Funções principais**:
  - `load_csv_data`: Carrega dados de um arquivo CSV.
  - `load_json_data`: Carrega dados de um arquivo JSON.

### 2. Limpeza dos Dados (`data_cleaning.py`)

Aqui é onde a mágica acontece. Os dados extraídos são limpos e transformados para ficarem padronizados.

- **Funções principais**:
  - `clean_data`: Remove valores nulos.
  - `normalize_data`: Formata strings para minúsculas e ajusta datas.
  - `aggregate_data`: Faz a agregação dos dados, somando valores por categorias.
  - `filter_data`: Filtra os dados com base em condições, como status ativo.
  - `convert_data_types`: Converte os tipos de dados, como datas e números.

### 3. Carregamento dos Dados (`etl_process.py`)

Esse script é o cérebro do processo ETL, juntando tudo e garantindo que os dados sejam carregados no ClickHouse e no Supabase. Ele também faz o carregamento em lotes para não sobrecarregar os bancos.

### 4. Utilitários dos Bancos de Dados

#### ClickHouse (`clickhouse_utils.py`)

Aqui estão as funções que fazem a interação com o banco de dados ClickHouse:

- `store_data_in_clickhouse`: Insere os dados em lotes no ClickHouse.
- `test_clickhouse_connection`: Testa a conexão com o ClickHouse.

#### Supabase (`supabase_utils.py`)

Funções para armazenar e testar a conexão com o Supabase:

- `store_data_in_supabase`: Faz o upload dos dados limpos para o Supabase.
- `test_supabase_connection`: Verifica se a conexão com o Supabase está ativa.

### 5. Tratamento de Erros e Repetições (`connections.py` e `retry.py`)

- **Lógica de Repetição**: Tenta novamente se a conexão com o banco falhar, seguindo um backoff exponencial (tempo de espera aumenta a cada tentativa).
- **Logs**: Todos os erros são registrados nos logs para facilitar a identificação de problemas.

### Logs e Rastreabilidade

O processo de ETL é todo logado. Você vai ter logs para cada etapa, desde a extração até o carregamento no banco de dados. Se acontecer algum erro, o processo de inserção é revertido, o erro é registrado e a conexão é fechada de forma adequada. Isso garante que o processo seja mais seguro e fácil de depurar.

## Exemplo de Uso


```
pip install -r requirements
cd src
python3 etl_process.py
```


## Conclusão

Esse pipeline ETL é pensado para ser flexível e fácil de escalar. Cada etapa do processo foi modularizada para facilitar a manutenção e a identificação de possíveis problemas. Com a lógica de repetição e tratamento de erros, o processo garante a integridade dos dados e oferece segurança na manipulação de grandes volumes.

