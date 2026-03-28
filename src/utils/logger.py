import json
import logging
import logging.handlers
import os
from datetime import datetime, timezone

from src.config.config import Config


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
            "logger": record.name,
            "nivel": record.levelname,
            "funcao": record.funcName,
            "linha": record.lineno,
            "mensagem": record.getMessage(),
        }
        if record.exc_info:
            payload["erro"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class LoggerConfig:
    _loggers = {}

    @staticmethod
    def setup_logging(name: str, log_file: str = None) -> logging.Logger:
        if name in LoggerConfig._loggers:
            return LoggerConfig._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(Config.LOG_LEVEL)
        logger.propagate = False
        logger.handlers.clear()

        formatter = JsonFormatter()

        console_handler = logging.StreamHandler()
        console_handler.setLevel(Config.LOG_LEVEL)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        arquivo_log = log_file or Config.LOG_FILE
        os.makedirs(os.path.dirname(arquivo_log), exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            arquivo_log,
            maxBytes=10_485_760,
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setLevel(Config.LOG_LEVEL)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        LoggerConfig._loggers[name] = logger
        return logger
