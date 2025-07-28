# file: src/constants.py
# author: tlynx538 (Vinayak Jaiwant Mooliyil)
# --- Constants for Chunk Size Calculation (Highly Recommended) ---
# Define base chunk sizes
_CHUNK_SIZE_SMALL_FILE = 256 * 1024  # 256 KB for files < 10MB
_CHUNK_SIZE_MEDIUM_FILE = 1 * 1024 * 1024 # 1 MB for files < 100MB
_CHUNK_SIZE_LARGE_FILE = 4 * 1024 * 1024 # 4 MB for files < 1GB
_CHUNK_SIZE_VERY_LARGE_FILE = 8 * 1024 * 1024 # 8 MB for files >= 1GB

# File size thresholds
_FILE_SIZE_THRESHOLD_SMALL = 10 * 1024 * 1024  # 10 MB
_FILE_SIZE_THRESHOLD_MEDIUM = 100 * 1024 * 1024 # 100 MB
_FILE_SIZE_THRESHOLD_LARGE = 1 * 1024 * 1024 * 1024 # 1 GB


# Memory safety parameters (consider making these configurable via config system)
# Absolute maximum chunk size to prevent excessively large individual reads/writes
_MAX_ABSOLUTE_CHUNK_SIZE = 64 * 1024 * 1024 # 64 MB
# Absolute minimum chunk size to prevent excessive overhead per request
_MIN_ABSOLUTE_CHUNK_SIZE = 64 * 1024 # 64 KB

# Heuristics for dynamic adjustment (can be tuned or made configurable)
_MEDIA_FILE_CHUNK_MULTIPLIER = 2
_MAX_MEDIA_FILE_CHUNK_SIZE = 16 * 1024 * 1024 # 16 MB

_MEMORY_SAFETY_FACTOR_DENOMINATOR = 4 # Use 1/X of available memory for safety limit
_MAX_REASONABLE_CHUNKS = 200 # Limit to avoid too many HTTP requests for very small files

_DEFAULT_CHUNK_SIZE_FALLBACK = 4 * 1024 * 1024 # 4MB default if calculation fails


# ... (existing chunk size constants) ...

# --- Constants for Max Workers Calculation ---
_DEFAULT_MAX_WORKERS_FALLBACK = 4 # Default if calculation fails or on platforms without loadavg

# Conservative memory estimate per thread (tune carefully based on profiling)
# This should be an upper bound of memory consumed by a worker during its peak.
_ESTIMATED_MEM_PER_WORKER_BYTES = 16 * 1024 * 1024 # 16 MB per thread

# CPU-based worker calculation factors
_CPU_WORKER_MULTIPLIER = 2 # e.g., 2 * CPU_count for I/O-bound tasks
_MAX_LOAD_AVERAGE_FOR_FULL_CPU_UTIL = 1.0 # If loadavg exceeds this, workers might be reduced (or disabled)

# Absolute bounds for worker count
_MAX_WORKERS_ABSOLUTE_CAP = 32 # Hard upper limit to prevent system overload
_MIN_WORKERS_ABSOLUTE_FLOOR = 1 # At least one worker

# File size threshold for limiting workers for small files
_SMALL_FILE_WORKER_LIMIT_THRESHOLD = 10 * 1024 * 1024 # 10 MB
_MAX_WORKERS_FOR_SMALL_FILES = 4 # Max workers for files under the threshold


_HASH_CHUNK_SIZE_BYTES = 1024 * 1024 # 1 MB for hashing operations
_DEFAULT_HASH_VERIFICATION_FALLBACK = False # Hash verification result on error
_DEFAULT_CLEANUP_FAILED_WARN = True # Whether to log warnings on cleanup failure

# --- Constants for Download Chunks and Retries ---
_DOWNLOAD_MAX_RETRIES = 5
_DOWNLOAD_RETRY_DELAY_SECONDS = 2
_DOWNLOAD_TIMEOUT_SECONDS = 10
_DOWNLOAD_STREAM_CHUNK_SIZE_BYTES = 8192 # Default for iter_content

_CLI_REFRESH_RATE = 10 # updates per second for CLI
_CLI_STOP_TIMEOUT_SECONDS = 5 # seconds to wait for CLI thread to stop gracefully

_LOG_DIR_NAME = ".logs" # Directory name for log files

_TEMP_DIR_NAME = ".temp"

# Download settings
_DOWNLOAD_MAX_RETRIES = 5
_DOWNLOAD_RETRY_DELAY_SECONDS = 2
_DOWNLOAD_TIMEOUT_SECONDS = 10
_DOWNLOAD_STREAM_CHUNK_SIZE_BYTES = 1024 * 1024 # 1MB
_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5 # NEW: Timeout for ThreadPoolExecutor shutdown

# CLI Display Settings
_CLI_REFRESH_RATE = 4 # Updates per second
_CLI_STOP_TIMEOUT_SECONDS = 2 # How long to wait for CLI thread to joinoin