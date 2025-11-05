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

    try:

        log_etapa(etapa, "INFO", "Iniciando transformação dos dados...")

        # Exibir informação gerais
        log_etapa(etapa, "INFO", "Resumo inicial dos dados:")
        print(df.info())

        if df.empty:
            raise ValueError("O DataFrame está vazio. Nada para transformar.")

        # Add possíveis transformações

        log_etapa(etapa, "INFO",
                  f"Transformação sucedida. Total de linhas {len(df)}")
        return df

    except ValueError as e:
        log_etapa(etapa, "ERROR", str(e))
        raise

    # except Exception as e:
    #     log_etapa(etapa, "ERROR", f"Erro inesperado na transformação: {e}")
