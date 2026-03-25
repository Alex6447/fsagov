from loguru import logger


def setup_logging(log_file: str = "logs/log.log"):
    logger.add(log_file, level="DEBUG", rotation="10 MB", retention="7 days")
