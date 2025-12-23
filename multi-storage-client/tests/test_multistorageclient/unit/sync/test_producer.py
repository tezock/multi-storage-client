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
from datetime import datetime
from typing import Optional, cast

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.progress_bar import ProgressBar
from multistorageclient.sync.producer import ProducerThread
from multistorageclient.types import ObjectMetadata
from multistorageclient.utils import NullStorageClient
from test_multistorageclient.unit.utils import config


def _setup_test_clients(posix_profile: str, remote_profile: str, temp_posix, temp_remote):
    """Helper to set up test clients with profiles."""
    config.setup_msc_config(
        config_dict={
            "profiles": {
                posix_profile: temp_posix.profile_config_dict(),
                remote_profile: temp_remote.profile_config_dict(),
            }
        }
    )
    posix_client, _ = msc.resolve_storage_client(f"msc://{posix_profile}")
    remote_client, _ = msc.resolve_storage_client(f"msc://{remote_profile}")
    return posix_client, remote_client


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_producer_thread_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
    )

    producer_thread.start()
    producer_thread.join()

    assert not producer_thread.is_alive()
    assert producer_thread.error is not None


def test_progress_bar_update_in_producer_thread_without_deletion():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
        ObjectMetadata(key="file4.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file5.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file6.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
        delete_unmatched_files=False,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.pbar is not None
    assert progress.pbar.total == len(source_files)

    # Because file1.txt and file2.txt are the same, they should be skipped and the progress bar should be updated.
    assert progress.pbar.n == 2


def test_progress_bar_update_in_producer_thread_with_deletion():
    source_client = NullStorageClient()
    target_client = MockStorageClient()

    target_files = [
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
        delete_unmatched_files=True,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.pbar is not None
    assert progress.pbar.total == len(target_files)
    assert progress.pbar.n == 0


def test_producer_thread_with_shutdown_event():
    """Test ProducerThread respects shutdown event."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    # Create long list of files to ensure producer is interruptible
    source_files = [
        ObjectMetadata(key=f"file{i}.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0))
        for i in range(1_000_000)
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter([])  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=False)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
    )

    producer_thread.start()

    # Let it process a few files
    time.sleep(0.01)

    # Signal shutdown
    shutdown_event.set()

    # Wait for producer to stop
    producer_thread.join(timeout=1.0)

    # Producer thread should be stopped
    assert not producer_thread.is_alive()

    # Given that producer thread is not alive, the queue should have some files left
    queue_size = file_queue.qsize()
    assert queue_size > 0
