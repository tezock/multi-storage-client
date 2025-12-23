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
import os
import threading
from typing import TYPE_CHECKING, Optional

from ..progress_bar import ProgressBar
from ..types import ObjectMetadata
from ..utils import PatternMatcher
from .types import EventLike, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


class ProducerThread(threading.Thread):
    """
    A producer thread that compares source and target file listings to determine sync operations.

    This thread is responsible for iterating through both source and target storage locations,
    comparing their file listings, and queuing appropriate sync operations (ADD, DELETE, or STOP)
    for worker threads to process. It performs efficient merge-style iteration through sorted
    file listings to determine what files need to be synchronized.

    The thread compares files by their relative paths and metadata (content length,
    last modified time) to determine if files need to be copied, deleted, or can be skipped.

    The thread will put tuples of (OperationType, ObjectMetadata) into the file_queue.
    """

    def __init__(
        self,
        source_client: "AbstractStorageClient",
        source_path: str,
        target_client: "AbstractStorageClient",
        target_path: str,
        progress: ProgressBar,
        file_queue: QueueLike,
        num_workers: int,
        shutdown_event: EventLike,
        delete_unmatched_files: bool = False,
        pattern_matcher: Optional[PatternMatcher] = None,
        preserve_source_attributes: bool = False,
        follow_symlinks: bool = True,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
    ):
        super().__init__(daemon=True)
        self.source_client = source_client
        self.target_client = target_client
        self.source_path = source_path
        self.target_path = target_path
        self.progress = progress
        self.file_queue = file_queue
        self.num_workers = num_workers
        self.shutdown_event = shutdown_event
        self.delete_unmatched_files = delete_unmatched_files
        self.pattern_matcher = pattern_matcher
        self.preserve_source_attributes = preserve_source_attributes
        self.follow_symlinks = follow_symlinks
        self.source_files = source_files
        self.ignore_hidden = ignore_hidden
        self.error = None

    def _match_file_metadata(self, source_info: ObjectMetadata, target_info: ObjectMetadata) -> bool:
        # Check file size is the same and the target's last_modified is newer than the source.
        return (
            source_info.content_length == target_info.content_length
            and source_info.last_modified <= target_info.last_modified
        )

    def _is_hidden(self, path: str) -> bool:
        """Check if a path contains any hidden components (starting with dot)."""
        if not self.ignore_hidden:
            return False
        parts = path.split("/")
        return any(part.startswith(".") for part in parts)

    def _create_source_files_iterator(self):
        """Create an iterator from source_files that yields ObjectMetadata."""
        if self.source_files is not None:
            for rel_file_path in self.source_files:
                rel_file_path = rel_file_path.lstrip("/")
                source_file_path = os.path.join(self.source_path, rel_file_path).lstrip("/")
                try:
                    source_metadata = self.source_client.info(
                        source_file_path, strict=False
                    )  # don't check if the path is a directory
                    yield source_metadata
                except FileNotFoundError:
                    logger.warning(f"File in source_files not found at source: {source_file_path}")
                    continue

    def run(self):
        try:
            if self.source_files is not None:
                source_iter = iter(self._create_source_files_iterator())
            else:
                source_iter = iter(
                    self.source_client.list(
                        prefix=self.source_path,
                        show_attributes=self.preserve_source_attributes,
                        follow_symlinks=self.follow_symlinks,
                    )
                )

            target_iter = iter(self.target_client.list(prefix=self.target_path))
            total_count = 0

            source_file = next(source_iter, None)
            target_file = next(target_iter, None)

            while source_file or target_file:
                if self.shutdown_event.is_set():
                    logger.info("ProducerThread: Shutdown event detected, stopping file enumeration")
                    break

                # Update progress and count each pair (or single) considered for syncing
                self.progress.update_total(total_count)

                if source_file and target_file:
                    source_key = source_file.key[len(self.source_path) :].lstrip("/")
                    target_key = target_file.key[len(self.target_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(source_key):
                        source_file = next(source_iter, None)
                        continue

                    if self._is_hidden(target_key):
                        target_file = next(target_iter, None)
                        continue

                    if source_key < target_key:
                        # Check if file should be included based on patterns
                        if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                            self.file_queue.put((OperationType.ADD, source_file))
                            total_count += 1
                        source_file = next(source_iter, None)
                    elif source_key > target_key:
                        if self.delete_unmatched_files:
                            self.file_queue.put((OperationType.DELETE, target_file))
                            total_count += 1
                        target_file = next(target_iter, None)  # Skip unmatched target file
                    else:
                        # Both exist, compare metadata
                        if not self._match_file_metadata(source_file, target_file):
                            # Check if file should be included based on patterns
                            if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                                self.file_queue.put((OperationType.ADD, source_file))
                        else:
                            self.progress.update_progress()

                        source_file = next(source_iter, None)
                        target_file = next(target_iter, None)
                        total_count += 1
                elif source_file:
                    source_key = source_file.key[len(self.source_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(source_key):
                        source_file = next(source_iter, None)
                        continue

                    # Check if file should be included based on patterns
                    if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                        self.file_queue.put((OperationType.ADD, source_file))
                        total_count += 1
                    source_file = next(source_iter, None)
                elif target_file:
                    target_key = target_file.key[len(self.target_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(target_key):
                        target_file = next(target_iter, None)
                        continue

                    if self.delete_unmatched_files:
                        self.file_queue.put((OperationType.DELETE, target_file))
                        total_count += 1
                    target_file = next(target_iter, None)

            self.progress.update_total(total_count)
        except Exception as e:
            self.error = e
        finally:
            for _ in range(self.num_workers):
                self.file_queue.put((OperationType.STOP, None))  # Signal consumers to stop
