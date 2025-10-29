"""
Módulo principal da pipeline de Análise de Dados de Materiais de agricultura
-----------------------------------------------------------

Este script é o ponto de entrada do projeto e executa a primeira etapa da
pipeline ETL: a **extração** dos dados brutos armazenados na pasta `data/raw/`.

Etapas:
1. Configura o sistema de logs.
2. Chama a função de extração dos dados brutos (Extract).
3. Exibe no terminal o resultado da execução.

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

import logging
from src.etl.extract import extrair_dados


# Configuração de logging
def configurar_logs():
    """Configura o sistema de logging para console e arquivo."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=[
                            logging.FileHandler("logs/etl_pipelin.log",
                                                mode="a", encoding="utf-8"),
                            logging.StreamHandler()  # exibe no terminal também
                            ]
                        )


def main():
    """Executa a pipeline de ETL (por enquanto, apenas a etapa Extract)."""

    configurar_logs()
    caminho_arquivo = "data/raw/agricultural_raw_material.csv"

    logging.info("Iniciando pipeline de ETL...")

    try:
        df = extrair_dados(caminho_arquivo)
        logging.info("Extração concluída. Total de linhas: %s", len(df))
    except Exception as e:  # noqa: W0718
        logging.error("Falha inesperada na pipeline: %s", e)


if __name__ == "__main__":
    main()
