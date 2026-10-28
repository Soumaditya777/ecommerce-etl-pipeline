import logging

from .config import LOG_DIR


def setup_logging() -> logging.Logger:
    """Log pipeline events to the console and a file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ecommerce_pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(
            LOG_DIR / "pipeline.log",
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
