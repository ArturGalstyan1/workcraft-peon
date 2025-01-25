import importlib
import io
import logging
import sys
from collections.abc import Callable
from contextlib import contextmanager

import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_not_result,
    stop_after_attempt,
    wait_exponential,
)

from workcraft.models import Workcraft


def import_module_attribute(path: str):
    module_path, attr_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def is_request_successful(response: requests.Response):
    return 200 <= response.status_code < 300


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_not_result(is_request_successful),
)
def tenacious_request(req_fn: Callable[..., requests.Response]):
    return req_fn()


class TeeIO:
    """Captures output AND forwards it to original stdout/stderr"""

    def __init__(self, original_stream, string_buffer):
        self.original_stream = original_stream
        self.string_buffer = string_buffer

    def write(self, data):
        self.original_stream.write(data)
        self.string_buffer.write(data)

    def flush(self):
        self.original_stream.flush()
        self.string_buffer.flush()


@contextmanager
def capture_all_output(workcraft: Workcraft, task_id: str):
    """Captures stdout, stderr, logging, and loguru output"""
    string_buffer = io.StringIO()

    old_stdout = sys.stdout
    old_stderr = sys.stderr

    sys.stdout = TeeIO(old_stdout, string_buffer)
    sys.stderr = TeeIO(old_stderr, string_buffer)

    log_handler = logging.StreamHandler(string_buffer)
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)

    loguru_handler_id = logger.add(string_buffer, format="{message}")

    try:
        yield string_buffer
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        root_logger.removeHandler(log_handler)
        logger.remove(loguru_handler_id)

        logs = string_buffer.getvalue()

        req = requests.post(
            f"{workcraft.stronghold_url}/api/task/{task_id}/update",
            headers={
                "WORKCRAFT_API_KEY": workcraft.api_key,
                "accept": "application/json",
            },
            json={"logs": logs, "logs_set": True},
        )

        if req.status_code != 200:
            logger.error(f"Failed to update logs: {req.text}")

        string_buffer.close()
