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

from datetime import datetime
from typing import Optional, cast
from unittest import mock

import pytest

from multistorageclient.client import StorageClient
from multistorageclient.sync import SyncManager
from multistorageclient.types import ObjectMetadata


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_sync_function_return_producer_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )
    with pytest.raises(RuntimeError, match="Errors in sync operation:"):
        manager.sync_objects()


def test_sync_objects_commits_metadata_by_default():
    """Test that sync_objects calls commit_metadata when commit_metadata is True (default)."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )

    with mock.patch.object(target_client, "commit_metadata") as mock_commit:
        manager.sync_objects(num_worker_processes=1, commit_metadata=True)
        mock_commit.assert_called_once()


def test_sync_objects_skips_commit_when_commit_metadata_false():
    """Test that sync_objects does NOT call commit_metadata when commit_metadata is False."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )

    with mock.patch.object(target_client, "commit_metadata") as mock_commit:
        manager.sync_objects(num_worker_processes=1, commit_metadata=False)
        mock_commit.assert_not_called()
