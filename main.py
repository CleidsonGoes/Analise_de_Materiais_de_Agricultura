"""
Módulo: main.py
----------------
Script principal para execução da pipeline ETL (Extract, Transform, Load).

Este módulo orquestra as etapas do processo ETL:
1. Extração dos dados brutos (Extract)
2. (Futuro) Transformação dos dados (Transform)
3. (Futuro) Carregamento dos dados tratados (Load)

Todas as mensagens e erros são registrados com o logger padronizado
que inclui nome da etapa, cor por nível de log e saída simultânea no terminal
e no arquivo de log `logs/etl_pipeline.log`.
"""

from src.utils.logger import log_etapa
from src.etl.extract import extrair_dados
from src.etl.transform import transformar_dados


def main():
    """Executa a pipeline ETL completa."""

    etapa = "MAIN"
    caminho_arquivo = "data/raw/agricultural_raw_material.csv"

    log_etapa(etapa, "INFO", "Iniciando pipeline ETL...")

    try:
        # === ETAPA: EXTRACT ===
        dados = extrair_dados(caminho_arquivo)
        log_etapa(etapa, "INFO",
                  f"Extração de dados sucedida! Total de linhas: {len(dados)}")

        # === ETAPA: TRANSFORM ===
        transformados = transformar_dados(dados)
        log_etapa(etapa, "INFO",
                  f"Pipeline sucedida! Linhas finais: {len(transformados)}")

        # === Futuras etapas ===
        # carregar_dados(transformados)

        log_etapa(etapa, "INFO", "Pipeline ETL finalizada com sucesso!")

    except Exception as e:  # noqa: W0718
        log_etapa(etapa, "ERROR", f"Erro na execução da pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
