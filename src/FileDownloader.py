# file: src/FileDownloader.py
# author: tlynx538 (Vinayak Jaiwant Mooliyil)
import requests
import logging
import os
import hashlib
import math
import platform
import json
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue
from typing import Optional, Dict, Any, Set, List, Tuple
import time
import threading # For file_write_lock

from . import constants
from .DownloaderUtils import DownloaderUtils 
from .DownloadProgressCLI import DownloadProgressCLI

# --- Custom Exceptions ---
class DownloadInitializationError(Exception):
    """Custom exception for errors during FileDownloader initialization."""
class DownloadError(Exception):
    """Custom exception for general download failures."""

# --- FileDownloader Class ---
class FileDownloader:
    def __init__(self, url: str, path: str, sha256hash: Optional[str], overwrite_existing: bool = True):
        self.url = url
        self.path = path or os.getcwd()
        self.logger = logging.getLogger(self.__class__.__module__)
        
        self.chunk_tracker = Queue() # For signalling COMPLETED chunk IDs to CLI's overall count
        self.active_chunk_progress_updates = Queue() # For signalling CURRENT progress updates from active chunks to CLI's detailed table
        
        self.hash = sha256hash
        self.session = requests.Session()
        self.overwrite_existing = overwrite_existing
        self._is_closed = False
        
        self.file_name: Optional[str] = None
        self.file_headers: Dict[str, str] = {}
        self.total_chunk_length: int = 0
        self.total_file_size: int = 0 # Store total file size for easy access
        self.executor: Optional[ThreadPoolExecutor] = None
        
        # This was the missing line that caused the AttributeError!
        self.completed_on_resume: Set[str] = set() # Re-added for CLI compatibility

        # New for direct writing and resume
        self.target_file_handle: Optional[Any] = None # Will hold the open file object for the final download
        self.file_write_lock = threading.Lock() # To protect seek/write operations on shared file handle
        self.completed_ranges: List[Tuple[int, int]] = [] # List of (start_byte, end_byte) for completed ranges
        self.progress_file_path: Optional[str] = None

        url_hash = hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:10]
        self.output_dir = os.path.join(self.path, constants._TEMP_DIR_NAME, url_hash)
        
        self._single_threaded_current_bytes: int = 0 
        self.cli_progress: Optional[DownloadProgressCLI] = None 

        try:
            os.makedirs(self.output_dir, exist_ok=True)
            self.logger.debug(f"Ensured unique temporary directory: '{self.output_dir}'")
        except OSError as e:
            self.logger.critical(f"Failed to create temporary directory '{self.output_dir}': {e}")
            raise DownloadInitializationError(f"Cannot initialize downloader: Failed to create temp directory at '{self.output_dir}'.") from e

    def startDownload(self):
        try:
            response = self.session.head(url=self.url)
            response.raise_for_status()

            self.file_name = DownloaderUtils.get_filename(self.url)
            self.file_headers = response.headers
            self.total_file_size = int(self.file_headers.get('Content-Length', '0')) # Store total size

            full_file_path = os.path.join(self.path, self.file_name)
            self.progress_file_path = os.path.join(self.output_dir, f".{self.file_name}.progress.json")

            # Handle existing file and overwrite logic
            if os.path.exists(full_file_path):
                self.logger.warning(f"File already exists: {full_file_path}.")
                if self.overwrite_existing:
                    self.logger.info(f"Overwriting existing file: {full_file_path}")
                    try:
                        os.unlink(full_file_path)
                        # Also delete existing progress file if overwriting
                        if os.path.exists(self.progress_file_path):
                            os.unlink(self.progress_file_path)
                    except OSError as e:
                        self.logger.error(f"Failed to delete existing file {full_file_path} for overwrite: {e}")
                        raise DownloadError(f"Failed to delete existing file {full_file_path} for overwrite.") from e
                else:
                    self.logger.error(f"File already exists: {full_file_path}. Overwrite not allowed.")
                    raise FileExistsError(f"Download target file already exists: {full_file_path}")

            # Initialize CLI progress display
            self.cli_progress = DownloadProgressCLI(downloader_instance=self)
            self.cli_progress.start() 

            # Determine download strategy (multi-threaded or single-threaded)
            if self.file_headers.get('Accept-Ranges') == 'bytes':
                self.logger.info(f"Headers acquired. Starting multi-threaded download for {self.file_name}")
                try:
                    self._initialize_multi_threaded_download(full_file_path)
                    # After all chunks are theoretically downloaded, verify hash
                    if self.hash:
                        if not DownloaderUtils.verify_hash(full_file_path, self.hash):
                            self.logger.error("Hash verification failed for multi-threaded download!")
                            raise DownloadError("Hash verification failed.")
                        else:
                            self.logger.info("Hash verification passed.")

                except (ValueError, requests.exceptions.RequestException, DownloadError) as e:
                    self.logger.warning(f"Multi-threaded download failed ({e.__class__.__name__}: {e}). Falling back to single-threaded.")
                    # If multi-threaded fails, clean up the partially downloaded file and temp data
                    self._cleanup_partial_multi_threaded_download(full_file_path)
                    if hasattr(self, 'cli_progress') and self.cli_progress:
                        self.cli_progress.stop()
                    self.__singleThreadedDownload(full_file_path)
                except Exception as e:
                    self.logger.exception(f"An unhandled error occurred during multi-threaded download for {self.file_name}. Terminating.")
                    self._cleanup_partial_multi_threaded_download(full_file_path)
                    if hasattr(self, 'cli_progress') and self.cli_progress:
                        self.cli_progress.stop()
                    raise

            else:
                self.logger.warning("Server does not support Range requests. Downloading file in a single thread.")
                self.__singleThreadedDownload(full_file_path)

        except Exception as e:
            self.logger.error(f"An exception has occurred while initiating download or retrieving file headers for {self.url}: {e}", exc_info=True)
            if hasattr(self, 'cli_progress') and self.cli_progress:
                self.cli_progress.stop()
            raise

    def _initialize_multi_threaded_download(self, full_file_path: str):
        """Initializes and manages the multi-threaded download process."""
        if not self.cli_progress: 
            raise RuntimeError("CLI Progress not initialized before multi-threaded download.")

        total_bytes = self.total_file_size
        if total_bytes <= 0:
            self.logger.error(f"Invalid Content-Length: {total_bytes}. Must be a positive integer for multi-threaded download.")
            raise ValueError(f"Invalid Content-Length: {total_bytes}. Must be positive for multi-threaded download.")

        chunk_size, max_workers = self._get_optimal_settings(total_bytes)
        self.total_chunk_length = math.ceil(total_bytes / chunk_size)

        # Initialize completed_ranges from progress file if exists
        self._load_progress()
        self._mark_resumed_chunks_for_cli(chunk_size, total_bytes)

        # Open the target file once for all threads
        try:
            # Use 'r+b' for resume, 'wb' for fresh (or when overwriting).
            # If the file exists and is smaller than total_bytes, we'll expand it.
            # If it doesn't exist, 'wb' creates it.
            mode = 'r+b' if os.path.exists(full_file_path) else 'wb'
            self.target_file_handle = open(full_file_path, mode)
            self.logger.debug(f"Opened final file handle: {full_file_path} in mode '{mode}'")
            
            # Pre-allocate file size if file is newly created or too small
            current_file_size = os.path.getsize(full_file_path)
            if current_file_size < total_bytes:
                self.logger.info(f"Pre-allocating file size to {DownloaderUtils.format_bytes(total_bytes)}.")
                try:
                    # Linux/macOS specific for sparse file allocation
                    if hasattr(os, 'posix_fallocate'):
                        os.posix_fallocate(self.target_file_handle.fileno(), 0, total_bytes)
                    else:
                        # Fallback for Windows or systems without posix_fallocate: write a null byte at the end
                        self.target_file_handle.seek(total_bytes - 1)
                        self.target_file_handle.write(b'\0')
                        self.target_file_handle.flush() # Ensure it's written to disk
                        self.target_file_handle.seek(current_file_size) # Go back to where we started (or 0)
                except OSError as e:
                    self.logger.warning(f"Failed to pre-allocate file: {e}. Download will continue without pre-allocation.")

        except IOError as e:
            self.logger.critical(f"Failed to open/create final file '{full_file_path}': {e}")
            raise DownloadInitializationError(f"Cannot initialize downloader: Failed to open final file at '{full_file_path}'.") from e

        # The CLI is responsible for setting its total.
        self.cli_progress.all_chunk_ids = set(f"part{i}" for i in range(self.total_chunk_length))

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        futures = []
        for i in range(self.total_chunk_length):
            start = i * chunk_size
            end = min(start + chunk_size - 1, total_bytes - 1)
            part_id = f"part{i}"

            # Check if this chunk range is already marked as completed
            if self._is_chunk_completed((start, end)):
                self.logger.debug(f"Skipping already downloaded chunk range: {part_id} (bytes {start}-{end})")
                self.chunk_tracker.put(part_id) # Signal CLI that this part is done
                # Also signal to active_chunk_progress_updates to remove it if it was somehow stuck there
                self.active_chunk_progress_updates.put((part_id, -1, -1))
                continue

            future = self.executor.submit(
                self.downloadSplit,
                start, end, part_id, i + 1, expected_size=(end - start + 1)
            )
            futures.append(future)

        for future in futures:
            future.result() # Wait for all submitted downloads to complete (or raise exceptions)
        
        self.logger.info("All multi-threaded chunks processed. Finalizing download.")

    def _mark_resumed_chunks_for_cli(self, chunk_size: int, total_bytes: int):
        """Translates completed_ranges into part_ids for CLI resume display."""
        self.completed_on_resume.clear()
        for i in range(self.total_chunk_length):
            start_chunk = i * chunk_size
            end_chunk = min(start_chunk + chunk_size - 1, total_bytes - 1)
            if self._is_chunk_completed((start_chunk, end_chunk)):
                part_id = f"part{i}"
                self.completed_on_resume.add(part_id)
                self.logger.debug(f"CLI: Marking chunk '{part_id}' as completed for resume display.")
                self.chunk_tracker.put(part_id) # Signal CLI for overall count
                self.active_chunk_progress_updates.put((part_id, -1, -1)) # Remove from active table

        if self.completed_on_resume:
            self.logger.info(f"Resuming download. {len(self.completed_on_resume)} chunks already completed out of {self.total_chunk_length}.")
        else:
            self.logger.info("No completed chunks found. Starting fresh download.")


    def _load_progress(self):
        """Loads completed ranges from the progress file."""
        self.completed_ranges.clear()
        if os.path.exists(self.progress_file_path):
            try:
                with open(self.progress_file_path, 'r') as f:
                    self.completed_ranges = json.load(f)
                self.logger.info(f"Loaded {len(self.completed_ranges)} completed ranges from {self.progress_file_path}")
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Could not load progress file {self.progress_file_path}: {e}. Starting fresh.")
                self.completed_ranges = []
        else:
            self.logger.debug(f"No progress file found at {self.progress_file_path}.")

    def _save_progress(self):
        """Saves completed ranges to the progress file."""
        if not self.progress_file_path:
            return
        try:
            with open(self.progress_file_path, 'w') as f:
                json.dump(self.completed_ranges, f)
            self.logger.debug(f"Saved {len(self.completed_ranges)} completed ranges to {self.progress_file_path}")
        except IOError as e:
            self.logger.error(f"Could not save progress file {self.progress_file_path}: {e}")

    def _add_completed_range(self, start: int, end: int):
        """Adds a newly completed range and saves progress."""
        with self.file_write_lock: # Protect access to completed_ranges and saving
            # Basic merging of adjacent ranges could be implemented here for efficiency
            # For simplicity, just append for now
            self.completed_ranges.append((start, end))
            # Sort and merge overlapping/adjacent ranges (optional, but good for large fragmented downloads)
            # This is more complex than a simple append and requires a separate utility function.
            # Example: self.completed_ranges = DownloaderUtils.merge_ranges(self.completed_ranges)
            self._save_progress()

    def _is_chunk_completed(self, chunk_range: Tuple[int, int]) -> bool:
        """Checks if a given chunk range is fully covered by completed_ranges."""
        chunk_start, chunk_end = chunk_range
        # This is a naive check; a more robust solution would properly merge and check
        # overlapping completed ranges. For now, it assumes a one-to-one mapping
        # between expected chunks and completed ranges entries.
        for completed_start, completed_end in self.completed_ranges:
            if completed_start == chunk_start and completed_end == chunk_end:
                return True
        return False

    def _cleanup_partial_multi_threaded_download(self, full_file_path: str):
        """Cleans up partially downloaded files and progress file after a multi-threaded failure."""
        self.logger.warning(f"Cleaning up partial multi-threaded download for {full_file_path}")
        if os.path.exists(full_file_path):
            try:
                os.unlink(full_file_path)
            except OSError as e:
                self.logger.error(f"Failed to delete partial file {full_file_path}: {e}")
        if os.path.exists(self.progress_file_path):
            try:
                os.unlink(self.progress_file_path)
            except OSError as e:
                self.logger.error(f"Failed to delete progress file {self.progress_file_path}: {e}")
        DownloaderUtils.cleanup_temp_dir(self.output_dir) # Clean up the temp dir itself


    def __singleThreadedDownload(self, full_file_path: str):
        """Handles single-threaded file download."""
        if not self.cli_progress:
            raise RuntimeError("CLI Progress not initialized before single-threaded download.")

        total_bytes = self.total_file_size # Use stored total file size
        
        # This update will now happen from within _display_loop's initial setup
        chunk_size = DownloaderUtils.get_chunk_size(total_bytes, self.file_name) 

        downloaded_bytes_count = 0
        try:
            with self.session.get(self.url, stream=True) as r:
                r.raise_for_status()
                with open(full_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes_count += len(chunk)
                            self._single_threaded_current_bytes = downloaded_bytes_count
                            
                            # For single-threaded, push updates to active_chunk_progress_updates queue
                            # as if it were a single 'thread' working on 'part0'
                            self.active_chunk_progress_updates.put(
                                ("part0", downloaded_bytes_count, total_bytes)
                            )

            self.logger.info(f"Download complete: {self.file_name}")
            
            # Final update for CLI for single-threaded to ensure 100%
            self._single_threaded_current_bytes = total_bytes 
            # Signal "part0" as completed to trigger overall chunks_completed update and remove from active table
            self.active_chunk_progress_updates.put(("part0", -1, -1)) # Sentinel for completion
            self.chunk_tracker.put("part0") 

            if self.hash:
                if not DownloaderUtils.verify_hash(full_file_path, self.hash):
                    self.logger.error("Hash verification failed for single-threaded download")
                    try:
                        os.unlink(full_file_path)
                    except OSError as unlink_e:
                        self.logger.error(f"Failed to delete corrupted file {full_file_path} after hash verification failure: {unlink_e}")
                    raise Exception("Hash verification failed")
                else:
                    self.logger.info("Hash verification passed")

        except (requests.exceptions.RequestException, OSError) as e:
            self.logger.error(f"Failed to download the file in single-threaded mode: {e}", exc_info=True)
            # Clean up partial file on error for single-threaded
            if os.path.exists(full_file_path):
                try:
                    os.unlink(full_file_path)
                except OSError as unlink_e:
                    self.logger.error(f"Failed to delete partial file {full_file_path} after error: {unlink_e}")
            raise
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during single-threaded download: {e}")
            raise

    def downloadSplit(self, start: int, end: int, part_id: str, section: int, expected_size: int):
        """Downloads a specific byte range (chunk) and writes it directly to the final file."""
        max_retries = constants._DOWNLOAD_MAX_RETRIES
        initial_retry_delay = constants._DOWNLOAD_RETRY_DELAY_SECONDS
        request_timeout = constants._DOWNLOAD_TIMEOUT_SECONDS
        stream_buffer_size = constants._DOWNLOAD_STREAM_CHUNK_SIZE_BYTES

        log_prefix = f"Chunk {part_id} (bytes {start}-{end}, section {section}/{self.total_chunk_length})"
        
        # Determine how many bytes we've already written if resuming mid-chunk
        # This isn't based on a separate temp file anymore, but what the actual file has.
        # However, for simplicity with current resume model (full chunks only), 
        # we treat as 0 initially if not a fully completed range.
        downloaded_bytes_in_chunk = 0 # This tracks progress *within the current attempt for this chunk*

        # For multi-threaded, if a chunk is being re-downloaded (e.g., due to previous error),
        # we start from scratch for that chunk range as per the resume_model assumption.
        # The 'Range' header will always reflect the start of the desired download for the chunk.

        for attempt in range(max_retries):
            self.logger.debug(f"{log_prefix} - Attempt {attempt + 1}/{max_retries}")

            # 'Range' header for the request, always for the full chunk initially if not resumed mid-chunk
            headers = {
                'Range': f'bytes={start}-{end}',
                'User-Agent': 'Mozilla/5.0'
            }

            try:
                # With direct writing, there's no temp_path specific to this chunk.
                # We always write to the main file.
                
                # Make the request for the chunk
                with self.session.get(
                    url=self.url,
                    headers=headers,
                    stream=True,
                    timeout=request_timeout
                ) as response:
                    response.raise_for_status()

                    # Ensure we get a 206 Partial Content if ranges were requested
                    if response.status_code == 200:
                        # If server ignores Range header, it sends whole file. Handle this fallback.
                        self.logger.warning(f"{log_prefix} - Server returned 200 OK, not 206 Partial Content. This chunk download might be incorrect or server doesn't fully support ranges as expected.")
                        # This scenario would require re-evaluating the whole download strategy or failing.
                        # For now, let's assume valid 206 or a recoverable 200 means we just write.
                        # However, if total_bytes > expected_size, it's problematic.
                        # For now, we will still write the range we asked for.
                        
                    current_write_offset = start # This is the absolute offset in the final file
                    downloaded_bytes_in_chunk = 0 # Reset for this attempt

                    for chunk in response.iter_content(chunk_size=stream_buffer_size):
                        if chunk:
                            # Use the lock to ensure only one thread seeks and writes at a time
                            # for the shared file handle, avoiding potential race conditions
                            # if the underlying OS buffering isn't perfectly distinct for different offsets.
                            with self.file_write_lock:
                                self.target_file_handle.seek(current_write_offset)
                                self.target_file_handle.write(chunk)
                                # Consider flushing periodically if needed, but the final flush
                                # in close() or exit() should handle it.
                                # self.target_file_handle.flush() # Can add this for stronger guarantees

                            current_write_offset += len(chunk)
                            downloaded_bytes_in_chunk += len(chunk)

                            # NEW: Update active chunk progress queue
                            # Periodically push updates, not on every small chunk, for performance
                            if (downloaded_bytes_in_chunk % (stream_buffer_size * 2) == 0 or 
                                downloaded_bytes_in_chunk >= expected_size):
                                self.active_chunk_progress_updates.put(
                                    (part_id, downloaded_bytes_in_chunk, expected_size)
                                )

                    # After successful download of this chunk's data, add to completed ranges
                    self._add_completed_range(start, end)

                    self.active_chunk_progress_updates.put((part_id, -1, -1)) # Sentinel for completion
                    self.chunk_tracker.put(part_id) # Signal overall CLI progress
                    self.logger.debug(f"{log_prefix} - Successfully downloaded and wrote directly.")
                    return # Chunk successfully downloaded and written

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"{log_prefix} - Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    delay = initial_retry_delay * (2 ** attempt)
                    self.logger.info(f"{log_prefix} - Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    raise Exception(f"Failed to download chunk {part_id} after {max_retries} attempts.") from e

            except IOError as e:
                self.logger.exception(f"{log_prefix} - File write operation failed: {e}.")
                raise # Re-raise I/O errors immediately, they are critical

            except Exception as e:
                self.logger.exception(f"{log_prefix} - An unexpected error occurred: {e}")
                raise

        self.logger.error(f"{log_prefix} - Permanently failed after {max_retries} attempts (fallthrough).")
        raise Exception(f"Failed to download chunk {part_id} after {max_retries} attempts (fallthrough).")


    def combineSplits(self) -> bool:
        """
        For multi-threaded downloads, this method now only performs hash verification.
        For single-threaded fallback, it's still responsible for the actual file write.
        """
        # Drain any remaining progress updates from the active_chunk_progress_updates queue
        while not self.active_chunk_progress_updates.empty():
            try:
                self.active_chunk_progress_updates.get_nowait()
            except Exception: # Queue might become empty between checks
                pass

        try:
            while not self.chunk_tracker.empty():
                try:
                    self.chunk_tracker.get_nowait()
                    self.chunk_tracker.task_done()
                except Exception:
                    pass

            full_file_path = os.path.join(self.path, self.file_name)
            
            # For multi-threaded download, the file is already combined. Just verify hash.
            if self.file_headers.get('Accept-Ranges') == 'bytes':
                # Ensure the file handle is flushed and closed before verification
                if self.target_file_handle:
                    self.target_file_handle.flush()
                    self.target_file_handle.close()
                    self.target_file_handle = None
                
                # Verify final file size
                if not os.path.exists(full_file_path) or os.path.getsize(full_file_path) != self.total_file_size:
                    self.logger.error(f"Combined file {full_file_path} is missing or has incorrect size.")
                    return False

                if self.hash:
                    if not DownloaderUtils.verify_hash(full_file_path, self.hash):
                        self.logger.error("Hash verification failed! File may be corrupted.")
                        try:
                            os.unlink(full_file_path)
                        except OSError as unlink_e:
                            self.logger.error(f"Failed to delete corrupted file {full_file_path} after hash verification failure: {unlink_e}")
                        return False
                    self.logger.info("Hash verification passed")
                
                # Cleanup progress file after successful download and verification
                if os.path.exists(self.progress_file_path):
                    try:
                        os.unlink(self.progress_file_path)
                        self.logger.debug(f"Cleaned up progress file: {self.progress_file_path}")
                    except OSError as e:
                        self.logger.error(f"Failed to delete progress file {self.progress_file_path}: {e}")

                return True
            else:
                # This else block should theoretically not be reached if __singleThreadedDownload
                # correctly handles its own file writing and hash verification.
                # However, if it were still merging temp files, this would be its logic.
                # Since __singleThreadedDownload now writes directly, this part is largely moot.
                self.logger.warning("combineSplits called for single-threaded download but file should already be complete.")
                # We can just return True assuming single-threaded was successful
                return True

        except Exception as e:
            self.logger.error(f"Unexpected error in combineSplits: {e}")
            return False

    def _get_optimal_settings(self, total_size: int):
        """Get optimal chunk size and worker count for this download."""
        chunk_size = DownloaderUtils.get_chunk_size(total_size, self.file_name)
        max_workers = DownloaderUtils.get_max_workers(total_size)
        return chunk_size, max_workers

    def __enter__(self):
        self.logger.debug(f"Entering FileDownloader context for URL: {self.url}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug(f"Exiting FileDownloader context for URL: {self.url}. Performing cleanup.")
        
        # Ensure target file handle is closed
        if self.target_file_handle:
            try:
                self.target_file_handle.close()
                self.logger.debug("Final target file handle closed.")
            except Exception as e:
                self.logger.error(f"Error closing final target file handle: {e}")
            self.target_file_handle = None

        if exc_type is None:
            # If no exception, and it was a multi-threaded download, clean up temp dir.
            # If single-threaded, no specific temp dir beyond main output_dir.
            # The progress file should be deleted by combineSplits on success.
            if hasattr(self, 'output_dir') and self.file_headers.get('Accept-Ranges') == 'bytes':
                DownloaderUtils.cleanup_temp_dir(self.output_dir)
        else:
            self.logger.warning(f"Keeping temporary files/progress in '{self.output_dir}' due to an exception ({exc_type.__name__}).")
            # If an error occurred during multi-threaded download, the partial file and progress file are kept for potential resume.
            if self.file_headers.get('Accept-Ranges') == 'bytes':
                self._save_progress() # Save current progress on error
            
        self.close()
        # Return False to propagate the exception if any
        if exc_type is not None:
            return False

    def close(self) -> None:
        if self._is_closed:
            self.logger.debug("FileDownloader is already closed. Skipping redundant cleanup.")
            return

        self.logger.info(f"Closing FileDownloader and cleaning up resources for URL: {self.url}")

        if self.executor:
            self.logger.debug("Attempting to shut down ThreadPoolExecutor.")
            self.executor.shutdown(wait=True, cancel_futures=True) # Wait for futures to complete or cancel
            self.executor = None

        if hasattr(self, 'session'):
            try:
                self.session.close()
                self.logger.debug("Requests session closed.")
            except Exception as e:
                self.logger.error(f"Error closing requests session: {e}")
        else:
            self.logger.warning("Requests session was not initialized. Skipping session close.")
        
        if hasattr(self, 'cli_progress') and self.cli_progress:
            self.cli_progress.stop()

        self._is_closed = True
        self.logger.info(f"FileDownloader resources for URL: {self.url} cleaned up.")