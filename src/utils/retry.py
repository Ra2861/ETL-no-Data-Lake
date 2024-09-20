import time
import logging

import time
import logging

MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

def retry_on_failure(func):
  """
  Decorador para refazer uma função em caso de falha.

  Este decorador envolve a função fornecida e a reexecuta até um número máximo
  de tentativas caso ocorra uma exceção.

  Parâmetros:
      func (callable): Função a ser decorada.

  Retorna:
      callable: Função decorada com a lógica de reexecução.
  """

  def wrapper(*args, **kwargs):
    retries = 0
    while retries < MAX_RETRIES:
      try:
        return func(*args, **kwargs)
      except Exception as e:
        retries += 1
        logging.error(f"Erro em {func.__name__}: {e}. Tentativa {retries}/{MAX_RETRIES}")
        time.sleep(RETRY_DELAY)  # Espera antes de tentar novamente

    logging.critical(f"Falha crítica: a função {func.__name__} falhou após {MAX_RETRIES} tentativas.")
    raise Exception(f"Function {func.__name__} failed after {MAX_RETRIES} attempts.")

  return wrapper
