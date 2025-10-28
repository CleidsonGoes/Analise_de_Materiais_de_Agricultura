"""
Módulo principal da pipeline de Análise de Dados de Materiais de agricultura
-----------------------------------------------------------

Este script é o ponto de entrada do projeto e executa a primeira etapa da
pipeline ETL: a **extração** dos dados brutos armazenados na pasta `data/raw/`.

Etapas futuras:
- Transformação: limpeza e padronização dos dados.
- Carga: exportação dos dados tratados para a pasta `data/processed/` ou para
um banco de dados.

Execução:
----------
python main.py

Autor: Cleidson Goes
Data: 2025-10-27
"""

from src.etl.extract import extrair_dados


def main():
    """Função principal que orquestra a execução da etapa de extração."""

    caminho = "data/raw/agricultural_raw_material.csv"
    df = extrair_dados(caminho)

    print(df.head())


if __name__ == "__main__":
    main()
