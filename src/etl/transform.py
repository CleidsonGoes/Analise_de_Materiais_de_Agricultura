"""
Módulo: transform.py
---------------------
Responsável pela etapa de **Transformação (Transform)** da pipeline de dados.

Este módulo contém a função `transformar_dados`, que realiza:
- Limpeza de valores ausentes e inválidos
- Conversão de tipos de dados
- Padronização de colunas e formatação de datas

Etapa correspondente ao "T" do processo ETL (Extract, Transform, Load).
"""

import pandas as pd
from src.utils.logger import log_etapa


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza e transformação dos dados extraídos.

    Parâmetros:
        df (pd.DataFrame): Dados extraídos da etapa anterior (Extract).

    Retorna:
        pd.DataFrame: Dados limpos e transformados, prontos para análise.

    Raises:
        ValueError: Se o DataFrame estiver vazio.
    """

    etapa = "TRANSFORM"
    log_etapa(etapa, "INFO", "Iniciando transformação dos dados...")
