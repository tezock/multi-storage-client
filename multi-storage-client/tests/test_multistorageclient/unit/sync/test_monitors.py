# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import queue
import threading
import time
from typing import Optional, cast

from multistorageclient.client import StorageClient
from multistorageclient.progress_bar import ProgressBar
from multistorageclient.sync.monitors import ErrorMonitorThread, ResultMonitorThread
from multistorageclient.sync.types import ErrorInfo, OperationType


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_result_consumer_exits_with_stop_signal():
    target_client = MockStorageClient()
    result_queue = queue.Queue()

    result_consumer_thread = ResultMonitorThread(
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        result_queue=result_queue,
    )

    result_consumer_thread.start()
    result_consumer_thread.join(timeout=1)

    assert result_consumer_thread.is_alive()

    result_queue.put((OperationType.STOP, None, None))
    result_consumer_thread.join(timeout=1)

    assert not result_consumer_thread.is_alive()


def test_error_info_dataclass():
    """Test ErrorInfo dataclass can be created with proper fields."""
    error_info = ErrorInfo(
        worker_id="process-123-thread-0",
        exception_type="ValueError",
        exception_message="Invalid value",
        traceback_str="Traceback...",
        file_key="test.txt",
        operation="add",
    )

    assert error_info.worker_id == "process-123-thread-0"
    assert error_info.exception_type == "ValueError"
    assert error_info.exception_message == "Invalid value"
    assert error_info.traceback_str == "Traceback..."
    assert error_info.file_key == "test.txt"
    assert error_info.operation == "add"


def test_error_consumer_thread_fail_fast():
    """Test ErrorConsumerThread signals shutdown on first error."""
    error_queue = queue.Queue()
    shutdown_event = threading.Event()

    error_consumer = ErrorMonitorThread(
        error_queue=error_queue,
        shutdown_event=shutdown_event,
    )
    error_consumer.start()

    # Send an error
    error_info = ErrorInfo(
        worker_id="test-worker",
        exception_type="TestException",
        exception_message="Test error",
        traceback_str="Test traceback",
        file_key="test.txt",
        operation="add",
    )
    error_queue.put(error_info)

    # Wait for error consumer to process
    time.sleep(0.1)

    # Shutdown event should be set on first error
    assert shutdown_event.is_set()
    assert len(error_consumer.errors) == 1
    assert error_consumer.errors[0].worker_id == "test-worker"

    # Stop the consumer
    error_queue.put(None)
    error_consumer.join(timeout=1)
    assert not error_consumer.is_alive()
