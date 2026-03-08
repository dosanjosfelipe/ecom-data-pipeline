import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging() -> None:
    logger = logging.getLogger()

    if logger.handlers:
        return

    base_path = Path(__file__).parent.parent.parent
    logs_path = base_path / 'logs' / 'pipeline.log'
    logs_path.parent.mkdir(parents=True, exist_ok=True)

    logger_format = '%(asctime)s | %(filename)s | %(funcName)s | %(levelname)s | %(message)s'

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(logger_format))

    file_handler = RotatingFileHandler(
        logs_path,
        maxBytes=5_000_000,
        backupCount=3
    )
    file_handler.setFormatter(logging.Formatter(logger_format))

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
