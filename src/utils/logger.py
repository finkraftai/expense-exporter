import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
DEBUG_LOG_FILE = os.path.join(LOG_DIR, f"debug_log_{today}.log")
INFO_LOG_FILE = os.path.join(LOG_DIR, f"info_log_{today}.log")
WARNING_LOG_FILE = os.path.join(LOG_DIR, f"warning_log_{today}.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, f"error_log_{today}.log")

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
class LevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

def create_handler(file_path, level):
    handler = TimedRotatingFileHandler(
        file_path, when="midnight", interval=1, backupCount=3, encoding="utf-8"
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    handler.addFilter(LevelFilter(level))
    return handler

# Handlers
debug_handler = create_handler(DEBUG_LOG_FILE, logging.DEBUG)
info_handler = create_handler(INFO_LOG_FILE, logging.INFO)
warning_handler = create_handler(WARNING_LOG_FILE, logging.WARNING)
error_handler = create_handler(ERROR_LOG_FILE, logging.ERROR)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)

# Root logger
logger = logging.getLogger("expense_exporter")
logger.setLevel(logging.DEBUG)
logger.addHandler(debug_handler)
logger.addHandler(info_handler)
logger.addHandler(warning_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)




















