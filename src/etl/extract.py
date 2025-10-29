"""
Módulo: extract.py
------------------
Responsável pela etapa de **Extração (Extract)** da pipeline de dados.

Este módulo contém a função `extrair_dados`, que lê um arquivo CSV
armazenado em `data/raw/` e retorna seu conteúdo em um DataFrame do pandas.

Etapa correspondente ao "E" do processo ETL (Extract, Transform, Load).

Exemplo de uso:
---------------
from src.etl.extract import extrair_dados

dados = extrair_dados("data/raw/agricultural_raw_material.csv")
print(dados.head())
"""

import logging
import pandas as pd


def extrair_dados(caminho_arquivo: str) -> pd.DataFrame:
    """
    Extrai os dados de um arquivo CSV e retorna um Dataframe do pandas.

    Parâmetros:
        caminho_arquivo (str): Caminho completo do arquivo CSV.

    Retorna:
        pd.Dataframe: Dados carregados em formato tabular.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado.
        pd.errors.EmptyDataError: Se o arquivo estiver vazio.
        pd.errors.ParserError: Se o arquivo estiver corrompido ou mal formatado
    """

    logging.info("Iniciando extração de dados do arquivo: %s", caminho_arquivo)

    try:
        df = pd.read_csv(caminho_arquivo)
        logging.info("Extração sucedida. Total de linhas: %s", len(df))
        return df

    except FileNotFoundError as e:
        logging.error("Arquivo não encontrado: %s", caminho_arquivo)
        raise e

    except pd.errors.EmptyDataError as e:
        logging.error("O arquivo %s está vazio.", caminho_arquivo)
        raise e

    except pd.errors.ParserError as e:
        logging.error("Erro ao analisar o arquivo CSV: %s", caminho_arquivo)
        raise e
