"""
Módulo: logger.py
-----------------
Responsável pela configuração e padronização dos logs de toda a pipeline ETL.

Este módulo cria um sistema de logging que:
- Registra todas as mensagens em um arquivo `logs/etl_pipeline.log`;
- Exibe mensagens coloridas no terminal, diferenciando por tipo
    (INFO, ERROR, WARNING etc.);
- Inclui o nome da etapa do pipeline (EXTRACT, TRANSFORM, LOAD) em cada log;
- Facilita a rastreabilidade e depuração das etapas da pipeline.

Principais componentes:
------------------------
- logger: instância principal do `logging.Logger` usada em todo o projeto.
- log_etapa(etapa, level, msg): função auxiliar para registrar
    mensagens de log formatadas.

Formato do log:
---------------
 No arquivo `logs/etl_pipeline.log`:
    YYYY-MM-DD HH:MM:SS - LEVEL - [ETL] - ETAPA: <NOME> - Mensagem

 No terminal (colorido):
    HH:MM:SS - LEVEL - [ETL] - ETAPA: <NOME> - Mensagem

Exemplo de uso:
---------------
from src.utils.logger import log_etapa

log_etapa("EXTRACT", "info", "Iniciando extração de dados.")
log_etapa("TRANSFORM", "error", "Erro ao processar coluna 'valor_venda'.")

Autor: Cleidson Goes
Data: 2025-10-27
"""

import logging
from colorama import Fore, Style, init

init(autoreset=True)

file_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
    "%Y-%m-%d %H:%M:%S"
)


class ColorFormatter(logging.Formatter):
    """
    Classe: ColorFormatter
    ----------------------
    Responsável por aplicar cores às mensagens de log exibidas no terminal,
    de acordo com o nível de severidade (INFO, WARNING, ERROR, etc.).

    Essa classe herda de `logging.Formatter` e sobrescreve o método
    `format()` para adicionar códigos ANSI que definem as cores do texto.

    O objetivo é melhorar a legibilidade dos logs no terminal,
    sem afetar os logs gravados em arquivo (que permanecem sem formatação).

    Cores utilizadas:
    -----------------
    - DEBUG: Cinza
    - INFO: Verde
    - WARNING: Amarelo
    - ERROR: Vermelho
    - CRITICAL: Vermelho em negrito

    Métodos:
    --------
    - format(record): sobrescreve o método base para aplicar a cor conforme
        o nível do log.

    Exemplo de uso:
    ---------------
    handler_console = logging.StreamHandler()
    handler_console.setFormatter(ColorFormatter("%(message)s"))
    """

    COLORS = {
        "DEBUG": Fore.CYAN,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.MAGENTA
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, "")
        message = super().format(record)
        return f"{log_color}{message}{Style.RESET_ALL}"


console_formatter = ColorFormatter(
    "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
    "%H:%M:%S"
)

file_handler = logging.FileHandler("logs/etl_pipeline.log", mode="a")
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)

logger = logging.getLogger("ETL")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def log_etapa(etapa: str, level: str, msg: str):
    """Loga uma mensagem formatada com a etapa e o nível informado."""
    message = f"ETAPA: {etapa.upper()} - {msg}"
    getattr(logger, level.lower())(message)
