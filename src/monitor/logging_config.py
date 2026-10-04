import logging
from pathlib import Path

def get_logger(process_name):

    # Crear carpeta logs si no existe
    Path("logs").mkdir(exist_ok=True)
    logger = logging.getLogger(process_name)
    logger.setLevel(logging.INFO)

    # Evita agregar handlers duplicados
    if not logger.handlers:

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
        )

        file_handler = logging.FileHandler(
            f"logs/{process_name}.log",
            encoding="utf-8"
        )

        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger