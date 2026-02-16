import json
import logging
import os
from datetime import datetime


class CustomFormatter(logging.Formatter):
    """Base class for custom logger."""

    def __init__(self, logging_level):
        """Initialise a custom logger."""
        self.logging_level = logging_level

        grey = "\x1b[38;20m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"

        log_format = (
            "%(asctime)s - %(levelname)s: %(message)s"
            if logging_level == logging.DEBUG
            else "%(message)s"
        )

        self.FORMATS = {
            logging.DEBUG: grey + log_format + reset,
            logging.INFO: grey + log_format + reset,
            logging.WARNING: yellow + log_format + reset,
            logging.ERROR: red + log_format + reset,
            logging.CRITICAL: bold_red + log_format + reset,
        }

    def format(self, record: logging.LogRecord) -> str:
        """Set the format of a log record.

        Returns:
            str: The formatted log record.

        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.

        Returns:
            str: JSON formatted log record.

        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def configure_console_logging(verbosity: int):
    """Initialise a logger with the specified log level."""
    logger = logging.getLogger("")
    logger.propagate = True
    logger.setLevel(logging.DEBUG)  # handlers filter the level

    # Override via env var
    if os.getenv("LOG_LEVEL") == "DEBUG":
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO if verbosity == 0 else max(2 - verbosity, 0) * 10

    # Remove any previously added StreamHandlers (but not subclasses like pytest's LogCaptureHandler)
    logger.handlers = [
        h for h in logger.handlers if type(h) is not logging.StreamHandler
    ]
    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)

    # Use JSON format if LOG_FORMAT=json env var is set
    if os.getenv("LOG_FORMAT") == "json":
        console_handler.setFormatter(JsonFormatter())
    else:
        console_handler.setFormatter(CustomFormatter(logging_level=loglevel))
    logger.addHandler(console_handler)
