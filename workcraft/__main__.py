import functools
import signal
import sys

import fire
from loguru import logger

from workcraft.models import Workcraft
from workcraft.peon import Peon
from workcraft.utils import import_module_attribute


peon: Peon | None = None


def signal_handler(signum, frame):
    global shutdown_flag, peon
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    if peon:
        peon.stop()

    sys.exit(0)


def main(workcraft_path: str):
    global shutdown_flag, peon
    signal_handler_partial = functools.partial(signal_handler)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_handler_partial)

    try:
        logger.info(f"Getting workcraft object at {workcraft_path}")
        workcraft: Workcraft = import_module_attribute(workcraft_path)
        peon = Peon(workcraft)
        peon.work()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt in main")
        if peon:
            peon.stop()
    finally:
        sys.exit(0)


if __name__ == "__main__":
    fire.Fire(main)
