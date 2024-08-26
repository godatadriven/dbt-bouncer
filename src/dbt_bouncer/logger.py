import logging
import os


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    log_format: str = "%(asctime)s - %(levelname)s: %(message)s"

    FORMATS = {
        logging.DEBUG: grey + log_format + reset,
        logging.INFO: grey + log_format + reset,
        logging.WARNING: yellow + log_format + reset,
        logging.ERROR: red + log_format + reset,
        logging.CRITICAL: bold_red + log_format + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def configure_console_logging(verbosity: int):
    logger = logging.getLogger("")
    logger.propagate = True
    logger.setLevel(logging.DEBUG)  # handlers filter the level

    # Override via env var
    if os.getenv("LOG_LEVEL") is not None:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO if verbosity == 0 else max(2 - verbosity, 0) * 10

    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)
