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

from pathlib import Path
import pandas as pd


def extrair_dados(caminho_arquivo: str) -> pd.DataFrame:
    """
    Extrai os dados de um arquivo CSV e retorna um Dataframe do pandas.

    Parâmetros:
        caminho_arquivo (str): Caminho completo do arquivo CSV.

    Retorna:
        pd.Dataframe: Dados extraidos.
    """

    caminho = Path(caminho_arquivo)

    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho.resolve()}")

    print(f"Extraindo dados de: {caminho.resolve()}")

    df = pd.read_csv(caminho)

    print(f"Extração sucedida! {df.shape[0]} linhas e {df.shape[1]} colunas.")

    return df
