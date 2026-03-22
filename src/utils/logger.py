from __future__ import annotations

import logging
import os
from typing import Any

from pythonjsonlogger import jsonlogger


def setup_logging(level: str | None = None) -> logging.Logger:
    """
    Configura logging estruturado (JSON) para o projeto.

    Level pode ser sobrescrito via LOG_LEVEL.
    """
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logger = logging.getLogger("engenharia_dados_prova")
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(module)s %(funcName)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def log_step(logger: logging.Logger, step: str, event: str, **fields: Any) -> None:
    logger.info({"step": step, "event": event, **fields})

