import logging
import sys


def init_basic_logger(
    name: str, level: int, with_tqdm: bool = False, file_handler: bool = False
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if len(logger.handlers) == 0 and not with_tqdm:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt="[%(asctime)s: %(levelname)s %(name)s] %(message)s")
        )
        logger.addHandler(handler)
    if file_handler:
        handler = logging.FileHandler(name + ".log")
        handler.setFormatter(
            logging.Formatter(fmt="[%(asctime)s: %(levelname)s %(name)s] %(message)s")
        )
        logger.addHandler(handler)
    return logger
