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

import contextlib
import json
import logging
import os
import shutil
import tempfile
import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union, cast

import xattr
from filelock import FileLock

from ..constants import MEMORY_LOAD_LIMIT
from ..providers.base import BaseStorageProvider
from ..types import ObjectMetadata
from .types import ErrorInfo, EventLike, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)

DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes
FILE_LOCK_SIZE_THRESHOLD = 64 * 1024 * 1024  # 64 MB - only lock files larger than this


def _sync_worker_process(
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    num_worker_threads: int,
    file_queue: QueueLike,
    result_queue: QueueLike,
    error_queue: QueueLike,
    shutdown_event: EventLike,
):
    """
    Worker process that handles file synchronization operations using multiple threads.

    This function is designed to run in a separate process as part of a multiprocessing
    sync operation. It spawns multiple worker threads that consume sync operations from
    the file_queue and perform the actual file transfers (ADD) or deletions (DELETE).

    Exceptions that occur during file operations are caught, packaged as ErrorInfo
    objects, and sent to the error_queue for centralized error handling. The shutdown_event
    is checked periodically to enable graceful shutdown when errors occur elsewhere.
    """

    def _sync_consumer(thread_id: int) -> None:
        """Processes files from the queue and copies them."""
        worker_id = f"process-{os.getpid()}-thread-{thread_id}"

        while True:
            if shutdown_event.is_set():
                logger.debug(f"Worker {worker_id}: Shutdown event detected, exiting")
                break

            op, file_metadata = file_queue.get()

            if op == OperationType.STOP:
                break

            source_key = file_metadata.key[len(source_path) :].lstrip("/")
            target_file_path = os.path.join(target_path, source_key)

            try:
                if op == OperationType.ADD:
                    logger.debug(f"sync {file_metadata.key} -> {target_file_path}")
                    with _create_exclusive_filelock(target_client, target_file_path, file_metadata.content_length):
                        # Since this is an ADD operation, the file doesn't exist in the metadata provider.
                        # Check if it exists physically to support resume functionality.
                        target_metadata = None
                        if not target_client._storage_provider:
                            raise RuntimeError("Invalid state, no storage provider configured.")

                        try:
                            # This enables "resume" semantics even when the object exists physically but is not yet
                            # present in the metadata provider (e.g., partial sync, interrupted run).
                            if target_client._metadata_provider:
                                resolved = target_client._metadata_provider.realpath(target_file_path)
                                if resolved.exists:
                                    # This should not happen but the check is to keep the code
                                    # consistent with write/upload behavior
                                    physical_path = resolved.physical_path
                                else:
                                    physical_path = target_client._metadata_provider.generate_physical_path(
                                        target_file_path, for_overwrite=False
                                    ).physical_path
                            else:
                                physical_path = target_file_path

                            # The physical path cannot be a directory, so we can use strict=False to avoid the check.
                            target_metadata = target_client._storage_provider.get_object_metadata(
                                physical_path, strict=False
                            )
                        except FileNotFoundError:
                            pass

                        # If file exists physically, check if sync is needed
                        if target_metadata is not None:
                            # First check if file is already up-to-date (optimization for all cases)
                            if (
                                target_metadata.content_length == file_metadata.content_length
                                and target_metadata.last_modified >= file_metadata.last_modified
                            ):
                                logger.debug(f"File {target_file_path} already exists and is up-to-date, skipping copy")

                                # Since this is an ADD operation, the file exists physically but not in metadata provider.
                                # Add it to metadata provider for tracking.
                                if target_client._metadata_provider:
                                    logger.debug(
                                        f"Adding existing file {target_file_path} to metadata provider for tracking"
                                    )
                                    with target_client._metadata_provider_lock or contextlib.nullcontext():
                                        target_client._metadata_provider.add_file(target_file_path, target_metadata)

                                continue

                            # If we reach here, file exists but needs updating - check if overwrites are allowed
                            if (
                                target_client._metadata_provider
                                and not target_client._metadata_provider.allow_overwrites()
                            ):
                                raise FileExistsError(
                                    f"Cannot sync '{file_metadata.key}' to '{target_file_path}': "
                                    f"file exists and needs updating, but overwrites are not allowed. "
                                    f"Enable overwrites in metadata provider configuration or remove the existing file."
                                )

                        source_physical_path, target_physical_path = _check_posix_paths(
                            source_client, target_client, file_metadata.key, target_file_path
                        )

                        source_is_posix = source_physical_path is not None
                        target_is_posix = target_physical_path is not None

                        if source_is_posix and target_is_posix:
                            _copy_posix_to_posix(source_physical_path, target_physical_path)
                            _update_posix_metadata(target_client, target_physical_path, target_file_path, file_metadata)
                        elif source_is_posix and not target_is_posix:
                            _copy_posix_to_remote(target_client, source_physical_path, target_file_path, file_metadata)
                        elif not source_is_posix and target_is_posix:
                            _copy_remote_to_posix(source_client, file_metadata.key, target_physical_path)
                            _update_posix_metadata(target_client, target_physical_path, target_file_path, file_metadata)
                        else:
                            _copy_remote_to_remote(
                                source_client, target_client, file_metadata.key, target_file_path, file_metadata
                            )

                    # Clean up the lock file for large files on POSIX file storage providers
                    if (
                        target_client._is_posix_file_storage_provider()
                        and file_metadata.content_length >= FILE_LOCK_SIZE_THRESHOLD
                    ):
                        try:
                            target_lock_file_path = os.path.join(
                                os.path.dirname(target_file_path), f".{os.path.basename(target_file_path)}.lock"
                            )
                            lock_path = cast(BaseStorageProvider, target_client._storage_provider)._prepend_base_path(
                                target_lock_file_path
                            )
                            os.remove(lock_path)
                        except OSError:
                            # Lock file might already be removed or not accessible
                            pass

                    # add tuple of (virtual_path, physical_metadata) to result_queue
                    if target_client._metadata_provider:
                        physical_metadata = target_client._metadata_provider.get_object_metadata(
                            target_file_path, include_pending=True
                        )
                    else:
                        physical_metadata = None
                    result_queue.put((op, target_file_path, physical_metadata))
                elif op == OperationType.DELETE:
                    logger.debug(f"rm {file_metadata.key}")
                    target_client.delete(file_metadata.key)
                    result_queue.put((op, target_file_path, None))
                else:
                    raise ValueError(f"Unknown operation: {op}")
            except Exception as e:
                if error_queue:
                    error_info = ErrorInfo(
                        worker_id=worker_id,
                        exception_type=type(e).__name__,
                        exception_message=str(e),
                        traceback_str=traceback.format_exc(),
                        file_key=file_metadata.key if file_metadata else None,
                        operation=op.value if op else "unknown",
                    )
                    error_queue.put(error_info)
                else:
                    logger.error(
                        f"Worker {worker_id}: Exception during {op} on {file_metadata.key}: {e}\n{traceback.format_exc()}"
                    )
                    raise

    # Worker process that spawns threads to handle syncing.
    threads = []
    for thread_id in range(num_worker_threads):
        thread = threading.Thread(target=_sync_consumer, args=(thread_id,), daemon=True)
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def _create_exclusive_filelock(
    target_client: "AbstractStorageClient",
    target_file_path: str,
    file_size: int,
) -> Union[FileLock, contextlib.AbstractContextManager]:
    """Create an exclusive file lock for large files on POSIX file storage providers.

    Acquires exclusive lock to prevent race conditions when multiple worker processes attempt concurrent
    writes to the same target location on shared filesystems. This can occur when users run multiple sync
    operations targeting the same filesystem location simultaneously.

    Uses size-based selective locking to balance performance and safety:
    - Small files (< 64MB): No lock - minimizes overhead for high-volume operations
    - Large files (≥ 64MB): Exclusive lock - prevents wasteful concurrent writes

    This approach avoids distributed lock overhead on parallel filesystems like Lustre
    when syncing millions of small files, while still protecting expensive large file transfers.
    """
    if target_client._is_posix_file_storage_provider() and file_size >= FILE_LOCK_SIZE_THRESHOLD:
        target_lock_file_path = os.path.join(
            os.path.dirname(target_file_path), f".{os.path.basename(target_file_path)}.lock"
        )
        lock_path = cast(BaseStorageProvider, target_client._storage_provider)._prepend_base_path(target_lock_file_path)
        return FileLock(lock_path, timeout=DEFAULT_LOCK_TIMEOUT)
    else:
        return contextlib.nullcontext()


def _check_posix_paths(
    source_client: "AbstractStorageClient",
    target_client: "AbstractStorageClient",
    source_key: str,
    target_key: str,
) -> tuple[Optional[str], Optional[str]]:
    """Check if source and target are POSIX paths and return physical paths if available."""
    source_physical_path = source_client.get_posix_path(source_key)
    target_physical_path = target_client.get_posix_path(target_key)
    return source_physical_path, target_physical_path


def _copy_posix_to_posix(
    source_physical_path: str,
    target_physical_path: str,
) -> None:
    """Copy file from POSIX source to POSIX target using shutil.copy2."""
    os.makedirs(os.path.dirname(target_physical_path), exist_ok=True)
    shutil.copy2(source_physical_path, target_physical_path)


def _copy_posix_to_remote(
    target_client: "AbstractStorageClient",
    source_physical_path: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Upload file from POSIX source to remote target."""
    target_client.upload_file(
        remote_path=target_file_path,
        local_path=source_physical_path,
        attributes=file_metadata.metadata,
    )


def _copy_remote_to_posix(
    source_client: "AbstractStorageClient",
    source_key: str,
    target_physical_path: str,
) -> None:
    """Download file from remote source to POSIX target."""
    source_client.download_file(remote_path=source_key, local_path=target_physical_path)


def _copy_remote_to_remote(
    source_client: "AbstractStorageClient",
    target_client: "AbstractStorageClient",
    source_key: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Copy file between two remote storages, using memory or temp file based on size."""
    if file_metadata.content_length < MEMORY_LOAD_LIMIT:
        file_content = source_client.read(source_key)
        target_client.write(target_file_path, file_content, attributes=file_metadata.metadata)
    else:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name
        try:
            source_client.download_file(remote_path=source_key, local_path=temp_filename)
            target_client.upload_file(
                remote_path=target_file_path,
                local_path=temp_filename,
                attributes=file_metadata.metadata,
            )
        finally:
            os.remove(temp_filename)


def _update_posix_metadata(
    target_client: "AbstractStorageClient",
    target_physical_path: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Update metadata for POSIX target (metadata provider or xattr)."""
    if target_client._metadata_provider:
        physical_metadata = ObjectMetadata(
            key=target_file_path,
            content_length=os.path.getsize(target_physical_path),
            last_modified=datetime.fromtimestamp(os.path.getmtime(target_physical_path), tz=timezone.utc),
            metadata=file_metadata.metadata,
        )
        with target_client._metadata_provider_lock or contextlib.nullcontext():
            target_client._metadata_provider.add_file(target_file_path, physical_metadata)
    else:
        if file_metadata.metadata:
            # Update metadata for POSIX target (xattr).
            try:
                xattr.setxattr(
                    target_physical_path,
                    "user.json",
                    json.dumps(file_metadata.metadata).encode("utf-8"),
                )
            except OSError as e:
                logger.debug(f"Failed to set extended attributes on {target_physical_path}: {e}")

        # Update (atime, mtime) for POSIX target.
        try:
            last_modified = file_metadata.last_modified.timestamp()
            os.utime(target_physical_path, (last_modified, last_modified))
        except OSError as e:
            logger.debug(f"Failed to update (atime, mtime) on {target_physical_path}: {e}")
