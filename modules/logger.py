import logging
import datetime
from pytz import timezone

def activate_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Converting to Finnish Time format
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%d.%m.%Y %H:%M:%S")
    formatter.converter = lambda *args: datetime.datetime.now(tz=timezone("Europe/Helsinki")).timetuple()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    #Only add handler the first time
    if len(logger.handlers) == 0:
        logger.addHandler(handler)
    
    logger.info(f"Autologger has been activated")
    return logger