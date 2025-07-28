# file: src/DownloaderUtils.py
# author: tlynx538 (Vinayak Jaiwant Mooliyil)
import os 
import shutil 
from urllib.parse import unquote, urlparse 
import re 
import psutil 
from hashlib import sha256
import logging 
from typing import Optional
from . import constants

_logger = logging.getLogger(__name__)

class DownloaderUtils:
    """Static utility methods for file downloading operations."""
    
    @staticmethod
    def get_filename(url: str, max_len: int = 255) -> str:
        try:
            """Extract and sanitize filename from URL."""
            if not isinstance(url, str) or not url:
                    _logger.warning("Invalid URL provided to get_filename. Returning default.")
                    return "download"
            parsed_url = urlparse(url).path 
            basename = unquote(os.path.basename(parsed_url))
            safe_name = re.sub(r'[^\w\-_.]', '', basename)

            if not safe_name:
                safe_name = "download"
                _logger.info(f"Filename derived from URL '{url}' resulted in an empty string after sanitization. Defaulting to '{safe_name}'.")
            
            if len(safe_name) > max_len:
                original_safe_name = safe_name # For logging context
                # Split extension if exists to shorten filename but retain extension
                name_part, ext_part = os.path.splitext(safe_name)
                # Shorten the name part, reserving space for extension and potential ellipsis
                if len(ext_part) > 0 and max_len - len(ext_part) > 0:
                    safe_name = name_part[:max_len - len(ext_part) - 3] + "..." + ext_part # -3 for ellipsis
                else:
                    safe_name = safe_name[:max_len] # Just truncate if no good place for ellipsis or ext

                _logger.warning(
                    f"Sanitized filename '{original_safe_name}' exceeded maximum length of {max_len}. "
                    f"Truncated to '{safe_name}'."
                )

            return safe_name or "download"
        except Exception as e:
            _logger.exception(
                f"An unexpected error occurred while extracting and sanitizing filename from URL: '{url}'. "
                f"Returning default filename."
            )
            return "download"
    
    @staticmethod
    def get_chunk_size(total_size: int, filename: str = "") -> int:
        """Calculate optimal chunk size based on file size and type."""
        # 1. Input Validation and Early Exit for invalid total_size
        if not isinstance(total_size, int) or total_size <= 0:
            _logger.error(
                f"Invalid total_size '{total_size}' provided to get_chunk_size. "
                f"Must be a positive integer. Falling back to default: {constants._DEFAULT_CHUNK_SIZE_FALLBACK} bytes."
            )
            return constants._DEFAULT_CHUNK_SIZE_FALLBACK

        try:
            # 2. Get system memory information (robustly handle psutil availability)
            available_mem = 0
            try:
                mem = psutil.virtual_memory()
                available_mem = mem.available
            except (ImportError, AttributeError, OSError) as e:
                _logger.warning(
                    f"Could not get system memory information via psutil ({e.__class__.__name__}: {e}). "
                    "Chunk size calculation will not factor in available memory."
                )
                # If psutil fails, available_mem remains 0, which means safe_mem_limit will be 0.
                # This ensures the min(chunk_size, safe_mem_limit) check doesn't overly restrict
                # when memory info is unavailable, relying on other bounds.

            # 3. Basic file type heuristics
            file_ext = os.path.splitext(filename)[1].lower()
            is_media_file = file_ext in {'.mp4', '.avi', '.mov', '.mkv', '.mp3', '.flac'}

            # 4. Base chunk size calculation based on total_size
            if total_size <= constants._FILE_SIZE_THRESHOLD_SMALL:
                chunk_size = constants._CHUNK_SIZE_SMALL_FILE
            elif total_size <= constants._FILE_SIZE_THRESHOLD_MEDIUM:
                chunk_size = constants._CHUNK_SIZE_MEDIUM_FILE
            elif total_size <= constants._FILE_SIZE_THRESHOLD_LARGE:
                chunk_size = constants._CHUNK_SIZE_LARGE_FILE
            else:
                chunk_size = constants._CHUNK_SIZE_VERY_LARGE_FILE

            # 5. Adjust for media files (if justified and desired)
            if is_media_file:
                chunk_size = min(chunk_size * constants._MEDIA_FILE_CHUNK_MULTIPLIER, constants._MAX_MEDIA_FILE_CHUNK_SIZE)
                _logger.debug(f"Adjusted chunk size for media file: {chunk_size} bytes.")

            # 6. Memory safety check (only if available_mem was successfully obtained)
            if available_mem > 0 and constants._MEMORY_SAFETY_FACTOR_DENOMINATOR > 0:
                # Use a fraction of available memory as a cap for a single chunk
                safe_mem_limit = available_mem // constants._MEMORY_SAFETY_FACTOR_DENOMINATOR
                if chunk_size > safe_mem_limit:
                    _logger.debug(f"Chunk size {chunk_size} limited by memory safety to {safe_mem_limit} bytes.")
                    chunk_size = safe_mem_limit
            
            # 7. Enforce reasonable overall bounds (absolute min and max)
            chunk_size = max(chunk_size, constants._MIN_ABSOLUTE_CHUNK_SIZE)
            chunk_size = min(chunk_size, constants._MAX_ABSOLUTE_CHUNK_SIZE)
            
            # 8. Avoid excessively many chunks for very small files (min_chunk_size must be positive)
            if constants._MAX_REASONABLE_CHUNKS > 0:
                # Calculate the minimum chunk size needed to not exceed _MAX_REASONABLE_CHUNKS
                # Ensure min_chunk_size is at least 1 to avoid ZeroDivisionError if total_size is very small
                min_chunk_size_based_on_count = max(1, total_size // constants._MAX_REASONABLE_CHUNKS)
                if chunk_size < min_chunk_size_based_on_count:
                     _logger.debug(f"Chunk size {chunk_size} adjusted up to {min_chunk_size_based_on_count} to limit total chunks.")
                     chunk_size = min_chunk_size_based_on_count

            _logger.debug(f"Calculated chunk size for {total_size} bytes: {chunk_size} bytes.")
            return chunk_size

        except Exception as e:
            # Catch any other unexpected errors during calculation.
            _logger.exception(
                f"An unexpected error occurred during get_chunk_size calculation for total_size={total_size}, filename='{filename}'. "
                f"Falling back to default: {constants._DEFAULT_CHUNK_SIZE_FALLBACK} bytes."
            )
            return constants._DEFAULT_CHUNK_SIZE_FALLBACK

    @staticmethod
    def get_max_workers(file_size: Optional[int] = None) -> int:
        """Calculate optimal number of worker threads."""
        try:
            # 1. Determine CPU count (default to 1 if unavailable)
            cpu_count = os.cpu_count() or constants._MIN_WORKERS_ABSOLUTE_FLOOR

            # 2. Memory-based worker calculation
            mem_based_threads = constants._MAX_WORKERS_ABSOLUTE_CAP # Default to cap if no memory info
            available_mem_bytes = 0
            try:
                mem = psutil.virtual_memory()
                available_mem_bytes = mem.available
                if constants._ESTIMATED_MEM_PER_WORKER_BYTES > 0: # Prevent ZeroDivisionError
                    mem_based_threads = available_mem_bytes // constants._ESTIMATED_MEM_PER_WORKER_BYTES
                else:
                    _logger.warning("constants._ESTIMATED_MEM_PER_WORKER_BYTES is zero or negative. Memory-based thread calculation skipped.")
            except (ImportError, AttributeError, OSError) as e:
                _logger.warning(
                    f"Could not get system memory information via psutil ({e.__class__.__name__}: {e}). "
                    "Memory-based worker calculation will use absolute cap."
                )
            # Ensure mem_based_threads is at least the floor
            mem_based_threads = max(mem_based_threads, constants._MIN_WORKERS_ABSOLUTE_FLOOR)
            _logger.debug(f"Memory-based threads: {mem_based_threads} (Available RAM: {available_mem_bytes / (1024*1024*1024):.2f} GB)")


            # 3. CPU-based worker calculation with optional load consideration
            cpu_based_threads = cpu_count * constants._CPU_WORKER_MULTIPLIER
            
            load_1min = 0.0 # Default if getloadavg is not available
            try:
                if hasattr(os, 'getloadavg'): # Check for POSIX compatibility
                    load_1min = os.getloadavg()[0]
                    # Apply load factor if current load exceeds a configured threshold
                    if load_1min > constants._MAX_LOAD_AVERAGE_FOR_FULL_CPU_UTIL:
                        load_factor = max(constants._MAX_LOAD_AVERAGE_FOR_FULL_CPU_UTIL, load_1min)
                        cpu_based_threads = int((cpu_count * constants._CPU_WORKER_MULTIPLIER) / load_factor)
                        _logger.debug(f"Load average ({load_1min:.2f}) is high. CPU-based threads adjusted to: {cpu_based_threads}")
                    else:
                        _logger.debug(f"Load average ({load_1min:.2f}) is normal. CPU-based threads: {cpu_based_threads}")
                else:
                    _logger.warning("os.getloadavg not available on this platform. CPU-based worker calculation will not factor in system load.")
            except (AttributeError, OSError) as e: # Catch if getloadavg is present but fails (e.g., permission)
                _logger.warning(f"Failed to get load average ({e.__class__.__name__}: {e}). CPU-based worker calculation will not factor in system load.")
            
            # Ensure CPU-based threads are at least the floor
            cpu_based_threads = max(cpu_based_threads, constants._MIN_WORKERS_ABSOLUTE_FLOOR)
            _logger.debug(f"CPU-based threads: {cpu_based_threads} (CPU count: {cpu_count})")


            # 4. Combine CPU and Memory considerations
            max_threads = min(cpu_based_threads, mem_based_threads)
            _logger.debug(f"Combined CPU and memory consideration result: {max_threads} threads.")


            # 5. Apply absolute bounds
            max_threads = min(max_threads, constants._MAX_WORKERS_ABSOLUTE_CAP)
            max_threads = max(max_threads, constants._MIN_WORKERS_ABSOLUTE_FLOOR)
            _logger.debug(f"After applying absolute caps: {max_threads} threads.")

            # 6. Limit for small files (if file_size is provided and valid)
            if file_size is not None:
                if not isinstance(file_size, int) or file_size < 0:
                    _logger.warning(f"Invalid file_size '{file_size}' provided. Skipping small file worker limit.")
                elif file_size < constants._SMALL_FILE_WORKER_LIMIT_THRESHOLD:
                    old_max_threads = max_threads
                    max_threads = min(max_threads, constants._MAX_WORKERS_FOR_SMALL_FILES)
                    if max_threads < old_max_threads:
                        _logger.debug(f"File size ({file_size} bytes) is small. Max workers limited to {max_threads}.")

            _logger.info(f"Final calculated max workers: {max_threads} for file size: {file_size}")
            return max_threads

        # --- Broad exception catch for any unforeseen issues ---
        except Exception as e:
            _logger.exception(
                f"An unexpected error occurred during get_max_workers calculation for file_size={file_size}. "
                f"Falling back to default: {constants._DEFAULT_MAX_WORKERS_FALLBACK} workers."
            )
            return constants._DEFAULT_MAX_WORKERS_FALLBACK

    @staticmethod
    def verify_hash(file_path: str, expected_hash: str) -> bool:
        """Verify file hash against expected value."""
        # 1. Input Validation for expected_hash
        if not expected_hash:
            _logger.debug(f"No expected hash provided for '{file_path}'. Hash verification skipped.")
            return True  # No hash to verify

        # 2. Input Validation for file_path
        if not isinstance(file_path, str) or not file_path:
            _logger.error(f"Invalid file_path '{file_path}' provided for hash verification.")
            return constants._DEFAULT_HASH_VERIFICATION_FALLBACK # Use constant for consistency

        if not os.path.exists(file_path):
            _logger.error(f"File not found at '{file_path}' for hash verification.")
            return constants._DEFAULT_HASH_VERIFICATION_FALLBACK

        if not os.path.isfile(file_path):
            _logger.error(f"Path '{file_path}' is not a regular file. Cannot verify hash.")
            return constants._DEFAULT_HASH_VERIFICATION_FALLBACK

        try:
            hasher = sha256() # Corrected import
            # Using constant for chunk size
            with open(file_path, 'rb') as f:
                while chunk := f.read(constants._HASH_CHUNK_SIZE_BYTES): # Using defined constant
                    hasher.update(chunk)
            calculated_hash = hasher.hexdigest()

            # Compare lowercase hashes for robustness
            comparison_result = calculated_hash == expected_hash.lower()
            if comparison_result:
                _logger.debug(f"Hash verification successful for '{file_path}'.")
            else:
                _logger.warning(
                    f"Hash verification failed for '{file_path}'. "
                    f"Calculated: {calculated_hash}, Expected: {expected_hash.lower()}"
                )
            return comparison_result

        except IOError as e:
            # Specific handling for file I/O errors (e.g., permissions, disk full)
            _logger.exception(f"File I/O error during hash verification for '{file_path}': {e}")
            return constants._DEFAULT_HASH_VERIFICATION_FALLBACK
        except Exception as e:
            # Catch any other unexpected errors during hashing process
            _logger.exception(f"An unexpected error occurred during hash verification for '{file_path}': {e}")
            return constants._DEFAULT_HASH_VERIFICATION_FALLBACK

    @staticmethod
    def cleanup_temp_dir(temp_dir: str) -> None:
        """Clean up temporary directory."""
        try:
            if os.path.exists(temp_dir):
                _logger.info(f"Attempting to clean up temporary directory: '{temp_dir}'")
                shutil.rmtree(temp_dir)
                _logger.debug(f"Successfully cleaned up temporary directory: '{temp_dir}'")
            else:
                _logger.debug(f"Temporary directory '{temp_dir}' does not exist. No cleanup needed.")
        except OSError as e: # Catch specific OS-level errors (permissions, locked files)
            _logger.exception(f"OS error during cleanup of temporary directory '{temp_dir}': {e}. This might indicate permission issues or locked files.")
            # Depending on policy, you might return False or raise here if cleanup failure is critical.
            # Current design returns None, so just logging.
        except Exception as e:
            # Catch any other unexpected errors during cleanup
            _logger.exception(f"An unexpected error occurred during cleanup of temporary directory '{temp_dir}': {e}")
            # Same as above, just logging.

    @staticmethod
    def format_bytes(bytes_value: int) -> str:
        """
        Formats a byte value into a human-readable string (B, KB, MB, GB, TB).
        """
        if bytes_value < 0:
            return "0 B"
        
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        i = 0
        while bytes_value >= 1024 and i < len(units) - 1:
            bytes_value /= 1024
            i += 1
        return f"{bytes_value:.2f} {units[i]}"