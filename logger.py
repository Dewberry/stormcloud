import logging
import json


def set_up_logger(filename: str = None):
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    if filename:
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        logger.addHandler(stream_handler)

    return logger


def log_to_json(fpath: str):
    file_logs = []
    with open(fpath, "r") as f:
        with open(f"{fpath}.json", "w") as jf:
            for line in f.readlines():
                jline = json.loads(str(line))
                file_logs.append(jline)

            json.dump(file_logs, jf)
