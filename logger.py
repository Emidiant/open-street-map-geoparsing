import logging
import colorlog


def logger_init(name="log_file") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    stream_handler = colorlog.StreamHandler()
    stream_handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(asctime)s  %(name)8s:%(levelname)8s: %(message)s'))
    logger.addHandler(stream_handler)
    return logger
