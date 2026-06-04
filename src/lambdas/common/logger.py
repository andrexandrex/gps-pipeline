import json
import logging
import os


class _JsonFormatter(logging.Formatter):
    _SKIP = frozenset(
        ("msg", "args", "levelname", "levelno", "pathname", "filename", "module",
         "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created",
         "msecs", "relativeCreated", "thread", "threadName", "processName",
         "process", "name", "message")
    )

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "level": record.levelname,
            "logger": record.name,
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        for key, val in record.__dict__.items():
            if key not in self._SKIP:
                entry[key] = val
        return json.dumps(entry, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    logger.propagate = False
    return logger
