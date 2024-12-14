import json
import threading
import uuid
from queue import Empty, Queue

import requests
from loguru import logger
from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect as sync_connect

from workcraft.models import State, Task, Workcraft


class Peon:
    def __init__(
        self,
        workcraft: Workcraft,
        id: str | None = None,
        queues: list[str] | None = None,
    ) -> None:
        self.id = id or uuid.uuid4().hex
        self.state = State(id=self.id, status="IDLE", queues=queues)

        self.workcraft = workcraft
        self.seen_tasks_in_memory = set()

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
        self._statistics_thread = threading.Thread(target=self._statistics, daemon=True)

        self._stop_event = threading.Event()
        self._task_cancelled = threading.Event()

    def _update_and_send_state(self, **kwargs) -> None:
        try:
            self.state = self.state.update_and_return(**kwargs)
            res = requests.post(
                self.workcraft.stronghold_url + f"/pi/peons/{self.id}/update",
                headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                json=self.state.to_stronghold(),
            )

            if 200 <= res.status_code < 300:
                logger.info(f"Peon {self.id} updated successfully")
            else:
                logger.error(f"Failed to update peon: {res.status_code} - {res.text}")
        except Exception as e:
            logger.error(f"Failed to send peon update: {e}")

    def work(self) -> None:
        logger.info("Starting peon...")
        self._keep_alive_thread.start()
        logger.info("Started keep alive thread")

        self._websocket_thread.start()
        logger.info("Started websocket thread")

        self._heartbeat_thread.start()
        logger.info("Started heartbeat thread")
        self._processor_thread.start()
        logger.info("Started processor thread")

        self._statistics_thread.start()
        logger.info("Started statistics thread")
        logger.info(f"Peon ID {self.id}")
        available_tasks = self.workcraft.tasks.keys()
        logger.info("Available Tasks:")
        for task in available_tasks:
            logger.info(f" - {task}")

        logger.success("Zug Zug. Ready to work!")
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1)  # Wait with timeout
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.stop()

    def _cancel_task_in_queue(self, task_id: str) -> None:
        # Remove task from queue
        for i in range(self.queue.qsize()):
            task = self.queue.get()
            if task.id != task_id:
                self.queue.put(task)
            else:
                logger.info(f"Removing task {task_id} from queue")
                task.status = "CANCELLED"

                res = requests.post(
                    f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    json=Task.to_stronghold(task),
                )

                if 200 <= res.status_code < 300:
                    logger.info(f"Task {task_id} removed from queue")
                else:
                    logger.error(f"Failed to remove task from queue: {res.text}")

                self.queue.task_done()
                break
        logger.info(f"Task {task_id} removed from queue")

    def _statistics(self) -> None:
        while self.working and not self._stop_event.is_set():
            # print stats for now
            # logger.info(f"Queue size: {self.queue.qsize()}")
            try:
                res = requests.post(
                    self.workcraft.stronghold_url + f"/api/peon/{self.id}/statistics",
                    json={
                        "type": "queue",
                        "value": {
                            "size": self.queue.qsize(),
                        },
                        "peon_id": self.id,
                    },
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                )

                if 200 <= res.status_code < 300:
                    # logger.info("Statistics sent successfully")
                    pass
                else:
                    logger.error(f"Failed to send statistics: {res.text}")
            except Exception as e:
                logger.error(f"Failed to send statistics: {e}")

            self._stop_event.wait(5)

    def _process(self) -> None:
        while self.working and not self._stop_event.is_set():
            if not self.websocket:
                self._stop_event.wait(5)
                continue
            try:
                task = self.queue.get(timeout=1)
            except Empty as _:
                if self.state.current_task:
                    self.state = self.state.update_and_return(
                        current_task=None, status="IDLE"
                    )
                    self._update_and_send_state()
                continue

            try:
                # logger.info(f"Processing task {task}, type: {type(task)}")
                self._update_and_send_state(current_task=task.id, status="WORKING")
                self._task_cancelled.clear()
                result_queue = Queue()

                def execute_task(_task):
                    try:
                        result = self.workcraft.execute(_task)
                        result_queue.put(result)
                    except Exception as e:
                        result_queue.put(e)

                task_thread = threading.Thread(target=execute_task, args=(task,))
                task_thread.start()

                # Monitor for cancellation or completion
                cancelled = False
                while task_thread.is_alive():
                    if (
                        self._task_cancelled.is_set() or self._stop_event.is_set()
                    ):  # Check both events
                        logger.info("Task cancellation requested")
                        task_thread.join(timeout=5)
                        if task_thread.is_alive():
                            logger.warning("Task did not stop gracefully")
                        task.status = "CANCELLED"
                        cancelled = True
                        break
                    task_thread.join(timeout=1)

                if not cancelled:
                    try:
                        updated_task = result_queue.get_nowait()
                    except Empty as e:
                        logger.error(
                            f"Failed to get task result: {e} because queue is empty"
                        )
                        task.status = "FAILURE"
                        task.result = (
                            f"Task failed to complete. No result available. Error: {e}"
                        )
                        updated_task = task
                else:
                    updated_task = task  # Use the cancelled task

                # Always try to send the final status, even if cancelled
                try:
                    res = requests.post(
                        f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json=Task.to_stronghold(updated_task),
                    )

                    if 200 <= res.status_code < 300:
                        logger.info(
                            f"Task updated successfully with status: {updated_task.status}"
                        )
                    else:
                        logger.error(
                            f"Failed to update task: {res.status_code} - {res.text}"
                        )
                except Exception as e:
                    logger.error(f"Failed to send task update: {e}")

            except Exception as e:
                logger.error(f"Failed to process task: {e}")
            finally:
                self._update_and_send_state(current_task=None, status="IDLE")
                self.seen_tasks_in_memory.remove(task.id)
                self.queue.task_done()

            # Break the loop if we're stopping
            if self._stop_event.is_set():
                logger.info("Stopping processor thread")
                break

    def _heartbeat(self) -> None:
        while self.working and not self._stop_event.is_set():
            if self.websocket:
                try:
                    self.websocket.send(json.dumps({"type": "heartbeat"}))
                    self._update_and_send_state()
                except Exception as e:
                    logger.error(f"Failed to send ping: {e}")
                    self.websocket = None
                self._stop_event.wait(5)  # Replace sleep with event wait

    def _keep_connection_alive(self) -> None:
        retry_delay = 1
        while self.working and not self._stop_event.is_set():
            if not self.websocket:
                logger.info("Reconnecting to websocket")
                try:
                    websocket_url = self.workcraft.websocket_url + self.id

                    if self.state.queues:
                        websocket_url += (
                            "&queues=['" + "','".join(self.state.queues) + "']"
                        )
                    else:
                        websocket_url += "&queues=NULL"

                    self.websocket = sync_connect(
                        # "wss://echo.websocket.org/"
                        websocket_url,
                        additional_headers={
                            "WORKCRAFT_API_KEY": self.workcraft.api_key
                        },
                    )
                    retry_delay = 1
                    logger.info("Reconnected to websocket")
                except Exception as e:
                    retry_delay = min(retry_delay * 2, 60)  # Cap at 60 seconds
                    logger.error(f"Failed to reconnect to websocket: {e}. Retrying...")
                    self._stop_event.wait(retry_delay)
            self._stop_event.wait(5)

    def _run_websocket(self):
        logger.info("Starting WebSocket thread")

        while self.working and not self._stop_event.is_set():
            if self.websocket:
                try:
                    msg = json.loads(self.websocket.recv(timeout=1.0))
                    # logger.info(f"Received message: {msg}")
                    if msg["type"] == "new_task":
                        task = Task.model_validate(msg["message"])

                        if task.id in self.seen_tasks_in_memory:
                            logger.info(f"Task {task.id} already seen, skipping")
                            continue

                        task.peon_id = self.id
                        self.queue.put(task)

                        self.seen_tasks_in_memory.add(task.id)

                        try:
                            res = requests.post(
                                self.workcraft.stronghold_url
                                + f"/api/task/{task.id}/acknowledgement",
                                headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                                json={"peon_id": self.id},
                            )

                            if 200 <= res.status_code < 300:
                                logger.info("Task acknowledgement sent successfully")
                            else:
                                logger.error(
                                    f"Failed to send task acknowledgement: {res.text}"
                                )
                        except Exception as e:
                            logger.error(f"Failed to send task acknowledgement: {e}")

                    if msg["type"] == "cancel_task":
                        task_id = msg["message"]
                        # either cancel the current task or remove from queue
                        if (
                            self.state.current_task
                            and self.state.current_task == task_id
                        ):
                            self._task_cancelled.set()
                            logger.info("Task cancellation acknowledged")
                        else:
                            self._cancel_task_in_queue(task_id)
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
        if not self.working:
            logger.info("Peon already shutting down...")
        else:
            logger.info("Initiating shutdown...")
            self.working = False
            self._stop_event.set()
            self._task_cancelled.set()
            # Set a timeout for joining threads
            timeout = 5
            threads = [
                self._websocket_thread,
                self._keep_alive_thread,
                self._heartbeat_thread,
                self._processor_thread,
                self._statistics_thread,
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

            # clean up the queue and set tasks back to PENDING

            while not self.queue.empty():
                try:
                    task = self.queue.get(timeout=1)
                    task.status = "PENDING"
                    task.peon_id = None

                    res = requests.post(
                        f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json=Task.to_stronghold(task),
                    )

                    if 200 <= res.status_code < 300:
                        logger.info(f"Task {task.id} reset to PENDING")
                    else:
                        logger.error(f"Failed to reset task {task.id}: {res.text}")

                    self.queue.task_done()

                except Exception as e:
                    logger.error(f"Failed to reset task: {e}")

            logger.info("Stopped peon")
