import json
import threading
import uuid
from queue import Queue

from loguru import logger
from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect as sync_connect

from workcraft.models import Task, Workcraft


class Peon:
    def __init__(self, workcraft: Workcraft, id: str | None = None) -> None:
        self.workcraft = workcraft
        self.id = id or uuid.uuid4().hex
        self.working = True

        self.queue = Queue()

        self.websocket: ClientConnection | None = None

        self._websocket_thread = threading.Thread(
            target=self._run_websocket, daemon=True
        )

        self._keep_alive_thread = threading.Thread(
            target=self._keep_connection_alive, daemon=True
        )

        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)

        self._processor_thread = threading.Thread(target=self._process, daemon=True)

        self._stop_event = threading.Event()

    def work(self) -> None:
        self._keep_alive_thread.start()
        self._websocket_thread.start()
        self._heartbeat_thread.start()
        self._processor_thread.start()
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1)  # Wait with timeout
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.stop()

    def _process(self) -> None:
        while self.working and not self._stop_event.is_set():
            if not self.websocket:
                self._stop_event.wait(5)
                continue
            try:
                task = self.queue.get(timeout=1)
            except Exception as _:
                continue

            try:
                logger.info(f"Processing task {task}, type: {type(task)}")
                updated_task = self.workcraft.execute(task)
                try:
                    self.websocket.send(
                        json.dumps(
                            {
                                "type": "task_done",
                                "message": Task.to_stronghold(updated_task),
                            }
                        )
                    )
                    logger.info(f"Task {task.id} processed successfully")
                except Exception as e:
                    logger.error(f"Failed to send task: {e}")

            except Exception as e:
                logger.error(f"Failed to process task: {e}")
            finally:
                self.queue.task_done

    def _heartbeat(self) -> None:
        while self.working and not self._stop_event.is_set():
            if self.websocket:
                try:
                    self.websocket.send(json.dumps({"type": "heartbeat"}))
                except Exception as e:
                    logger.error(f"Failed to send ping: {e}")
                    self.websocket = None
                self._stop_event.wait(5)  # Replace sleep with event wait

    def _keep_connection_alive(self) -> None:
        while self.working and not self._stop_event.is_set():
            if not self.websocket:
                logger.info("Reconnecting to websocket")
                try:
                    self.websocket = sync_connect(
                        # "wss://echo.websocket.org/"
                        self.workcraft.websocket_url + self.id,
                        additional_headers={
                            "WORKCRAFT_API_KEY": self.workcraft.api_key
                        },
                    )
                    logger.info("Reconnected to websocket")
                except Exception as e:
                    logger.error(f"Failed to reconnect to websocket: {e}. Retrying...")
                    self._stop_event.wait(5)
            self._stop_event.wait(5)

    def _run_websocket(self):
        logger.info("Starting WebSocket thread")
        while self.working and not self._stop_event.is_set():
            if self.websocket:
                try:
                    msg = json.loads(self.websocket.recv(timeout=1.0))
                    logger.info(f"Received message: {msg}")
                    if msg["type"] == "new_task":
                        task = Task.model_validate(msg["message"])
                        self.queue.put(task)
                        # send acknoledgement
                        self.websocket.send(
                            json.dumps({"type": "ack", "message": {"id": task.id}})
                        )
                except TimeoutError:
                    # Timeout is expected, continue the loop to check stop conditions
                    continue
                except ConnectionClosedOK:
                    logger.info("WebSocket connection closed normally")
                    self.websocket = None
                    continue
                except ConnectionClosed as e:
                    logger.info(f"WebSocket connection closed: {e}")
                    self.websocket = None
                    continue
                except Exception as e:
                    logger.error(f"Failed to receive message: {e}")
                    self.websocket = None
                    continue
        logger.info("WebSocket thread stopped")

    def stop(self):
        logger.info("Initiating shutdown...")
        self.working = False
        self._stop_event.set()

        # Set a timeout for joining threads
        timeout = 5
        threads = [
            self._websocket_thread,
            self._keep_alive_thread,
            self._heartbeat_thread,
            self._processor_thread,
        ]

        for thread in threads:
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(
                    f"Thread {thread.name} did not terminate within {timeout} seconds"
                )

        if self.websocket:
            try:
                self.websocket.close()
            except Exception as e:
                logger.error(f"Error closing websocket: {e}")

        logger.info("Stopped peon")
