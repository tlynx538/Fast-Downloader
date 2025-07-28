import requests
import logging
import os
from concurrent.futures import ThreadPoolExecutor, Future
import math
from queue import Queue
from typing import Optional, Dict, Any, Set
import time
import hashlib

from . import constants
from .DownloaderUtils import DownloaderUtils # Keep this for other DownloaderUtils methods


from .DownloadProgressCLI import DownloadProgressCLI

class DownloadInitializationError(Exception):
    """Custom exception for errors during FileDownloader initialization."""
class DownloadError(Exception):
    """Custom exception for general download failures."""


class FileDownloader:
    def __init__(self, url: str, path: str, sha256hash: Optional[str], overwrite_existing: bool = True):
        self.url = url
        self.path = path or os.getcwd()
        self.logger = logging.getLogger(self.__class__.__module__)
        self.chunk_tracker = Queue() # Queue for signalling COMPLETED chunk IDs to CLI's overall count
        # NEW: Queue for signalling CURRENT progress updates from active chunks to CLI's detailed table
        self.active_chunk_progress_updates = Queue() 
        self.hash = sha256hash
        self.session = requests.Session()
        self.overwrite_existing = overwrite_existing
        self._is_closed = False
        self.file_name: Optional[str] = None
        self.file_headers: Dict[str, str] = {}
        self.total_chunk_length: int = 0
        self.executor: Optional[ThreadPoolExecutor] = None
        self.completed_on_resume: Set[str] = set()

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

            full_file_path = os.path.join(self.path, self.file_name)

            if os.path.exists(full_file_path):
                self.logger.warning(f"File already exists: {full_file_path}.")
                if self.overwrite_existing:
                    self.logger.info(f"Overwriting existing file: {full_file_path}")
                    try:
                        os.unlink(full_file_path)
                    except OSError as e:
                        self.logger.error(f"Failed to delete existing file {full_file_path} for overwrite: {e}")
                        raise DownloadError(f"Failed to delete existing file {full_file_path} for overwrite.") from e
                else:
                    self.logger.error(f"File already exists: {full_file_path}. Overwrite not allowed.")
                    raise FileExistsError(f"Download target file already exists: {full_file_path}")

            # The CLI progress is now solely responsible for display.
            # Initializing it here, before any major download logic, to ensure it captures all updates.
            self.cli_progress = DownloadProgressCLI(downloader_instance=self)
            self.cli_progress.start() 


            if self.file_headers.get('Accept-Ranges') == 'bytes':
                self.logger.info(f"Headers acquired. Starting multi-threaded download for {self.file_name}")
                try:
                    self.initializeDownload() 
                    
                    if not self.combineSplits():
                        self.logger.error(f"Failed to combine splits for {self.file_name}.")
                        raise DownloadError(f"Multi-threaded download failed: could not combine splits for {self.file_name}.")
                except (ValueError, requests.exceptions.RequestException, DownloadError) as e:
                    self.logger.warning(f"Multi-threaded download failed ({e.__class__.__name__}: {e}). Falling back to single-threaded.")
                    DownloaderUtils.cleanup_temp_dir(self.output_dir) 
                    if hasattr(self, 'cli_progress') and self.cli_progress:
                        self.cli_progress.stop()
                    self.__singleThreadedDownload(full_file_path)
                except Exception as e:
                    self.logger.exception(f"An unhandled error occurred during multi-threaded download for {self.file_name}. Terminating.")
                    DownloaderUtils.cleanup_temp_dir(self.output_dir)
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


    def initializeDownload(self):
        if not self.cli_progress: 
            raise RuntimeError("CLI Progress not initialized before initializeDownload.")

        try:
            total_bytes_str = self.file_headers.get('Content-Length')
            if total_bytes_str is None:
                self.logger.error("Content-Length header is missing. Cannot perform multi-threaded download.")
                raise ValueError("Content-Length header is missing for multi-threaded download.")

            total_bytes = int(total_bytes_str)
            if total_bytes <= 0:
                self.logger.error(f"Invalid Content-Length: {total_bytes_str}. Must be a positive integer.")
                raise ValueError(f"Invalid Content-Length: {total_bytes_str}. Must be positive for multi-threaded download.")

            chunk_size, max_workers = self._get_optimal_settings(total_bytes)
            self.total_chunk_length = math.ceil(total_bytes / chunk_size)

            # This update will now happen from within _display_loop's initial setup
            # self.cli_progress.overall_progress_bar.update(
            #     self.cli_progress.overall_task,
            #     total=total_bytes,
            #     chunks_total=self.total_chunk_length
            # )
            
            # This is now only for the CLI's internal completed chunk count check
            self.cli_progress.all_chunk_ids = set(f"part{i}" for i in range(self.total_chunk_length))

            self.completed_on_resume.clear()
            if os.path.exists(self.output_dir):
                for i in range(self.total_chunk_length):
                    part_id = f"part{i}"
                    part_path = os.path.join(self.output_dir, part_id)
                    
                    if os.path.exists(part_path):
                        start = i * chunk_size
                        end = min(start + chunk_size - 1, total_bytes - 1)
                        expected_size = end - start + 1
                        actual_size = os.path.getsize(part_path)
                        
                        if actual_size == expected_size:
                            self.completed_on_resume.add(part_id)
                            self.logger.debug(f"Found completed chunk '{part_id}' (size: {actual_size}) for resume.")
                            # Signal CLI for overall count
                            self.chunk_tracker.put(part_id) 
                            # Also signal to active_chunk_progress_updates to remove it if it was somehow stuck there
                            self.active_chunk_progress_updates.put((part_id, -1, -1))
                        else:
                            self.logger.warning(f"Found incomplete or corrupted chunk '{part_id}' (expected: {expected_size}, actual: {actual_size}). Re-downloading.")
                            try:
                                os.unlink(part_path)
                            except OSError as e:
                                self.logger.error(f"Failed to delete corrupted chunk '{part_id}': {e}")
            
            if self.completed_on_resume:
                self.logger.info(f"Resuming download. {len(self.completed_on_resume)} chunks already completed out of {self.total_chunk_length}.")
            else:
                self.logger.info("No completed chunks found. Starting fresh download.")


            self.executor = ThreadPoolExecutor(max_workers=max_workers)
            futures = []
            for i in range(self.total_chunk_length):
                part_id = f"part{i}"
                
                if part_id in self.completed_on_resume:
                    self.logger.debug(f"Skipping already downloaded chunk: {part_id}")
                    continue

                start = i * chunk_size
                end = min(start + chunk_size - 1, total_bytes - 1)
                
                future = self.executor.submit(
                    self.downloadSplit,
                    start, end, part_id, i + 1, expected_size=end-start+1 # Pass expected size to downloadSplit
                )
                futures.append(future)

            for future in futures:
                future.result()

        except (KeyError, ValueError) as e:
            self.logger.error(f"Error determining total file size for multi-threaded download: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.exception(f"Failed during multi-threaded download initialization or execution: {e}")
            raise


    def __singleThreadedDownload(self, full_file_path: str):
        if not self.cli_progress:
            raise RuntimeError("CLI Progress not initialized before single-threaded download.")

        try:
            total_bytes_str = self.file_headers.get('Content-Length')
            if total_bytes_str is None:
                self.logger.warning("Content-Length header is missing for single-threaded download. Using default buffer size for streaming.")
                total_bytes = 0
            else:
                try:
                    total_bytes = int(total_bytes_str)
                except ValueError as e:
                    self.logger.warning(f"Invalid Content-Length '{total_bytes_str}' for single-threaded download. Using default buffer size for streaming. Error: {e}")
                    total_bytes = 0
            
            # This update will now happen from within _display_loop's initial setup
            # if self.cli_progress.overall_task is not None:
            #     self.cli_progress.overall_progress_bar.update(
            #         self.cli_progress.overall_task,
            #         total=total_bytes,
            #         chunks_total=1
            #     )
            #     self.cli_progress.all_chunk_ids.clear()
            #     self.cli_progress.all_chunk_ids.add("part0") 
                
            chunk_size = DownloaderUtils.get_chunk_size(total_bytes, self.file_name) 

            downloaded_bytes_count = 0
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
            raise
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during single-threaded download: {e}")
            raise


    def downloadSplit(self, start: int, end: int, part_id: str, section: int, expected_size: int):
        max_retries = constants._DOWNLOAD_MAX_RETRIES
        initial_retry_delay = constants._DOWNLOAD_RETRY_DELAY_SECONDS
        request_timeout = constants._DOWNLOAD_TIMEOUT_SECONDS
        stream_buffer_size = constants._DOWNLOAD_STREAM_CHUNK_SIZE_BYTES

        final_path = os.path.join(self.output_dir, part_id)
        
        # Check if this part was already completed since the initial scan
        if os.path.exists(final_path):
            if os.path.getsize(final_path) == expected_size:
                self.logger.debug(f"Chunk '{part_id}' already complete. Skipping re-download.")
                # Signal completion to active chunks queue as well, just in case it was in progress
                self.active_chunk_progress_updates.put((part_id, -1, -1)) 
                self.chunk_tracker.put(part_id)
                return
            else:
                self.logger.warning(f"Chunk '{part_id}' exists but is incomplete or corrupted. Re-downloading.")
                try:
                    os.unlink(final_path)
                except OSError as e:
                    self.logger.error(f"Failed to delete corrupted chunk '{part_id}': {e}")


        downloaded_bytes_in_chunk = 0

        for attempt in range(max_retries):
            log_prefix = f"Chunk {part_id} (bytes {start}-{end}, section {section}/{self.total_chunk_length})"
            self.logger.debug(f"{log_prefix} - Attempt {attempt + 1}/{max_retries}")

            headers = {
                'Range': f'bytes={start + downloaded_bytes_in_chunk}-{end}',
                'User-Agent': 'Mozilla/5.0'
            }

            try:
                temp_path = os.path.join(self.output_dir, f"{part_id}.tmp")
                
                if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                    mode = 'ab'
                    downloaded_bytes_in_chunk = os.path.getsize(temp_path)
                    self.logger.debug(f"{log_prefix} - Resuming download from {downloaded_bytes_in_chunk} bytes.")
                    headers['Range'] = f'bytes={start + downloaded_bytes_in_chunk}-{end}'

                else:
                    mode = 'wb'
                    downloaded_bytes_in_chunk = 0 

                if (start + downloaded_bytes_in_chunk) > end:
                    self.logger.warning(f"{log_prefix} - Local temp file '{temp_path}' suggests chunk is already larger than expected range. Finishing part assuming complete.")
                    os.replace(temp_path, final_path)
                    self.active_chunk_progress_updates.put((part_id, -1, -1)) # Sentinel for completion
                    self.chunk_tracker.put(part_id)
                    return

                with self.session.get(
                    url=self.url,
                    headers=headers,
                    stream=True,
                    timeout=request_timeout
                ) as response:
                    response.raise_for_status()

                    with open(temp_path, mode) as f:
                        for chunk in response.iter_content(chunk_size=stream_buffer_size):
                            if chunk:
                                f.write(chunk)
                                downloaded_bytes_in_chunk += len(chunk)
                                # NEW: Update active chunk progress queue
                                # Periodically push updates, not on every small chunk, for performance
                                if (downloaded_bytes_in_chunk % (stream_buffer_size * 2) == 0 or 
                                    downloaded_bytes_in_chunk == expected_size):
                                    self.active_chunk_progress_updates.put(
                                        (part_id, downloaded_bytes_in_chunk, expected_size)
                                    )

                    os.replace(temp_path, final_path)

                    self.active_chunk_progress_updates.put((part_id, -1, -1)) # Sentinel for completion
                    self.chunk_tracker.put(part_id)
                    self.logger.debug(f"{log_prefix} - Successfully downloaded and saved.")
                    return

            except OSError as e:
                self.logger.exception(f"{log_prefix} - File write/rename failed: {e}. Attempting cleanup of temp file.")
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                        self.logger.debug(f"Cleaned up incomplete temporary file: {temp_path}")
                    except OSError as cleanup_e:
                        self.logger.error(f"Failed to clean up temporary file {temp_path}: {cleanup_e}")
                raise

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"{log_prefix} - Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    delay = initial_retry_delay * (2 ** attempt)
                    self.logger.info(f"{log_prefix} - Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    raise Exception(f"Failed to download chunk {part_id} after {max_retries} attempts.") from e

            except Exception as e:
                self.logger.exception(f"{log_prefix} - An unexpected error occurred: {e}")
                raise

        self.logger.error(f"{log_prefix} - Permanently failed after {max_retries} attempts (fallthrough).")
        raise Exception(f"Failed to download chunk {part_id} after {max_retries} attempts (fallthrough).")


    def combineSplits(self) -> bool:
        # Drain any remaining progress updates from the active_chunk_progress_updates queue
        # to ensure the CLI's active_chunk_states is fully cleared before final display.
        while not self.active_chunk_progress_updates.empty():
            try:
                self.active_chunk_progress_updates.get_nowait()
                # logger.debug("Draining active_chunk_progress_updates queue during combineSplits.")
            except Exception: # Queue might become empty between checks
                pass

        try:
            while not self.chunk_tracker.empty():
                try:
                    self.chunk_tracker.get_nowait()
                    self.chunk_tracker.task_done()
                except Exception:
                    pass

            existing_parts = [
                f"part{i}" for i in range(self.total_chunk_length)
                if os.path.exists(os.path.join(self.output_dir, f"part{i}"))
            ]
            
            if len(existing_parts) != self.total_chunk_length:
                expected_chunks = set(f"part{i}" for i in range(self.total_chunk_length))
                existing_set = set(existing_parts)
                missing_chunks = expected_chunks - existing_set
                self.logger.error(f"Missing {len(missing_chunks)} chunks for combination: {sorted(list(missing_chunks))}")
                return False

            sorted_parts = sorted(existing_parts,
                                key=lambda x: int(x.replace("part", "")))

            full_file_path = os.path.join(self.path, self.file_name)
            temp_path = f"{full_file_path}.tmp"

            try:
                with open(temp_path, 'wb') as outfile:
                    for part_file in sorted_parts:
                        part_path = os.path.join(self.output_dir, part_file)
                        with open(part_path, 'rb') as pf:
                            while chunk := pf.read(1024 * 1024):
                                outfile.write(chunk)

                try:
                    os.replace(temp_path, full_file_path)
                except OSError as replace_e:
                    self.logger.warning(f"Atomic replace failed for '{temp_path}' to '{full_file_path}': {replace_e}. Attempting non-atomic rename as fallback.")
                    try:
                        os.rename(temp_path, full_file_path)
                    except OSError as rename_e:
                        self.logger.exception(f"Non-atomic rename also failed for '{temp_path}' to '{full_file_path}': {rename_e}.")
                        raise

                self.logger.info(f"Successfully assembled {full_file_path}")

                if self.hash:
                    if not DownloaderUtils.verify_hash(full_file_path,self.hash):
                        self.logger.error("Hash verification failed! File may be corrupted.")
                        try:
                            os.unlink(full_file_path)
                        except OSError as unlink_e:
                            self.logger.error(f"Failed to delete corrupted file {full_file_path} after hash verification failure: {unlink_e}")
                        return False
                    self.logger.info("Hash verification passed")

                return True

            except OSError as e:
                self.logger.error(f"File assembly failed: {e}")
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except OSError as cleanup_e:
                        self.logger.error(f"Failed to delete incomplete temporary file {temp_path} after assembly failure: {cleanup_e}")
                return False

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
        if exc_type is None:
            if hasattr(self, 'output_dir'):
                DownloaderUtils.cleanup_temp_dir(self.output_dir)
        else:
            self.logger.warning(f"Keeping temporary files in '{self.output_dir}' due to an exception ({exc_type.__name__}).")

        self.close()
        if exc_type is not None:
            return False

    def close(self) -> None:
        if self._is_closed:
            self.logger.debug("FileDownloader is already closed. Skipping redundant cleanup.")
            return

        self.logger.info(f"Closing FileDownloader and cleaning up resources for URL: {self.url}")

        if self.executor:
            self.logger.debug("Attempting to shut down ThreadPoolExecutor.")
            self.executor.shutdown(wait=False, cancel_futures=True) 
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