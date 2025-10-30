"""
Módulo: extract.py
------------------
Responsável pela etapa de **Extração (Extract)** da pipeline de dados.

Este módulo contém a função `extrair_dados`, que lê um arquivo CSV
armazenado em `data/raw/` e retorna seu conteúdo em um DataFrame do pandas.

Agora, utiliza o logger padronizado com cores e nome de etapa
(ETAPA: EXTRACT) para registrar cada ação e erro de forma consistente.

Etapa correspondente ao "E" do processo ETL (Extract, Transform, Load).
"""
import pandas as pd
from src.utils.logger import log_etapa


def extrair_dados(caminho_arquivo: str) -> pd.DataFrame:
    """
    Extrai os dados de um arquivo CSV e retorna um DataFrame do pandas.

    Parâmetros:
        caminho_arquivo (str): Caminho completo do arquivo CSV.

    Retorna:
        pd.DataFrame: Dados carregados em formato tabular.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado.
        pd.errors.EmptyDataError: Se o arquivo estiver vazio.
        pd.errors.ParserError: Se o arquivo estiver corrompido ou mal formatado
    """

    etapa = "EXTRACT"
    log_etapa(etapa, "INFO",
              f"Iniciando extração de dados do arquivo: {caminho_arquivo}")

    try:
        df = pd.read_csv(caminho_arquivo)
        log_etapa(etapa, "INFO",
                  f"Extração sucedida. Total de linhas: {len(df)}")
        return df

    except FileNotFoundError as e:
        log_etapa(etapa, "ERROR", f"Arquivo não encontrado: {caminho_arquivo}")
        raise e

    except pd.errors.EmptyDataError as e:
        log_etapa(etapa, "ERROR", f"O arquivo {caminho_arquivo} está vazio.")
        raise e

    except pd.errors.ParserError as e:
        log_etapa(etapa, "ERROR",
                  f"Erro ao analisar o arquivo CSV: {caminho_arquivo}")
        raise e

    except Exception as e:
        log_etapa(etapa, "ERROR", f"Erro inesperado na etapa de extração: {e}")
        raise
