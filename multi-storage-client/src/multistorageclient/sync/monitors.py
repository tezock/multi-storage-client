# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
from typing import TYPE_CHECKING

from ..progress_bar import ProgressBar
from .types import EventLike, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


class ResultMonitorThread(threading.Thread):
    """
    A monitor thread that processes sync operation results and updates metadata.

    This thread is responsible for consuming results from worker processes/threads
    that have completed sync operations (ADD or DELETE).
    """

    def __init__(
        self, target_client: "AbstractStorageClient", target_path: str, progress: ProgressBar, result_queue: QueueLike
    ):
        super().__init__(daemon=True)
        self.target_client = target_client
        self.target_path = target_path
        self.progress = progress
        self.result_queue = result_queue
        self.error = None

    def run(self):
        try:
            # Pull from result_queue to collect pending updates from each multiprocessing worker.
            while True:
                op, target_file_path, physical_metadata = self.result_queue.get()

                logger.debug(
                    f"ResultMonitorThread: {op}, target_file_path: {target_file_path}, physical_metadata: {physical_metadata}"
                )

                if op == OperationType.STOP:
                    break

                if op in (OperationType.ADD, OperationType.DELETE):
                    self.progress.update_progress()
        except Exception as e:
            self.error = e


class ErrorMonitorThread(threading.Thread):
    """
    A monitor thread that monitors and processes errors from worker threads.

    This thread is responsible for consuming error information from worker
    processes/threads that encounter exceptions during sync operations.
    On the first error, it signals graceful shutdown to stop further work.
    """

    def __init__(
        self,
        error_queue: QueueLike,
        shutdown_event: EventLike,
    ):
        super().__init__(daemon=True)
        self.error_queue = error_queue
        self.shutdown_event = shutdown_event
        self.errors: list = []
        self.error = None

    def run(self):
        try:
            while True:
                error_info = self.error_queue.get()

                if error_info is None:
                    break

                logger.error(
                    f"Error in worker {error_info.worker_id} during {error_info.operation} "
                    f"on file {error_info.file_key}: {error_info.exception_type}: {error_info.exception_message}"
                )

                self.errors.append(error_info)

                # Signal shutdown on first error (fail-fast)
                if len(self.errors) == 1:
                    logger.info("Error detected, signaling shutdown")
                    self.shutdown_event.set()

        except Exception as e:
            self.error = e
