import functools
import signal
import sys

import fire
from loguru import logger

from workcraft.models import Workcraft
from workcraft.peon import Peon
from workcraft.utils import import_module_attribute


def handle_shutdown(peon: Peon | None, signum: int, frame) -> None:
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    if peon:
        peon.stop()


def main(workcraft_path: str, queues: list[str] | None = None) -> None:
    peon = None
    try:
        logger.info(f"Getting workcraft object at {workcraft_path}")
        workcraft: Workcraft = import_module_attribute(workcraft_path)

        if queues:
            if isinstance(queues, tuple):
                logger.info(f"Converting queues to list: {queues}")
                queues = list(queues)
            elif isinstance(queues, str):
                logger.info(f"Converting queues to list: {queues}")
                queues = [queues]
            elif not isinstance(queues, list):
                raise ValueError(f"Invalid queues type: {type(queues)}")
            else:
                logger.info(f"Using queues: {queues}, type {type(queues)}")

        peon = Peon(workcraft, queues=queues)
        # Set up signal handlers with peon instance
        signal_handler = functools.partial(handle_shutdown, peon)
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Start work - this will block until stop() is called
        peon.work()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if peon:
            peon.stop()
        sys.exit(1)


if __name__ == "__main__":
    fire.Fire(main)
