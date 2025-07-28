# file: main.py
# author: tlynx538 (Vinayak Jaiwant Mooliyil)
import logging
import os
import sys
import argparse
import requests
import datetime
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src.FileDownloader import FileDownloader
from src import constants
from rich.logging import RichHandler

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

console_handler = RichHandler(
    show_time=True,
    show_level=True,
    show_path=False,
    enable_link_path=False,
    rich_tracebacks=True,
    log_time_format="%Y-%m-%d %H:%M:%S"
)
root_logger.addHandler(console_handler)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def setup_file_logging(log_dir: str):
    """Sets up a file handler for logging."""
    try:
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(log_dir, f"download_{timestamp}.log")

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file_path}")
    except OSError as e:
        logger.error(f"Failed to create log directory or file '{log_dir}': {e}. File logging disabled.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during file logging setup: {e}. File logging disabled.")


def run_downloader_cli():
    """
    Parses command-line arguments and initiates the file download.
    """
    parser = argparse.ArgumentParser(
        description="A multi-threaded file downloader with integrity checks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("url", help="The URL of the file to download.")
    parser.add_argument("-o", "--output", default=".", help="The output directory for the downloaded file.")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite the file if it already exists.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output (DEBUG level logging) to console.")
    parser.add_argument("--sha256", help="Expected SHA256 hash of the downloaded file for integrity verification.")
    parser.add_argument("--logs", action="store_true", help=f"Output logs to a file in the '{constants._LOG_DIR_NAME}' directory.")

    args = parser.parse_args()

    if args.verbose:
        console_handler.setLevel(logging.DEBUG)
        logger.debug("Verbose console output enabled.")
    
    if args.logs:
        log_dir_path = os.path.join(os.path.dirname(__file__), constants._LOG_DIR_NAME)
        setup_file_logging(log_dir_path)
        root_logger.setLevel(logging.DEBUG if args.verbose else logging.INFO) 

    download_dir = args.output
    try:
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Download directory '{download_dir}' ensured.")
    except OSError as e:
        logger.critical(f"Failed to create download directory '{download_dir}': {e}")
        sys.exit(1)

    downloader = None
    download_start_time = time.time()
    try:
        logger.info(f"Initiating download for: {args.url}")
        logger.info(f"Saving to: {os.path.abspath(download_dir)}")

        with FileDownloader(
            url=args.url,
            path=download_dir,
            sha256hash=args.sha256,
            overwrite_existing=args.force
        ) as downloader_instance:
            downloader = downloader_instance
            downloader.startDownload() # CLI starts inside here

        download_end_time = time.time()
        total_duration = download_end_time - download_start_time
        logger.info(f"Download of '{downloader.file_name}' completed successfully! Total duration: {total_duration:.2f}s")

    except FileExistsError as e:
        logger.error(f"Download failed: {e}. Use --force to overwrite existing files.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logger.error(f"A network or HTTP error occurred during download: {e}")
        logger.debug(f"Request Exception Details: {e}", exc_info=True)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Download interrupted by user (Ctrl+C). Attempting graceful shutdown...")
        # Ensure downloader cleanup is called before exiting
        if downloader:
            downloader.close() # This will stop CLI and executor
        sys.exit(2) # Then exit
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred during download: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # The `with FileDownloader(...) as downloader_instance:` context manager
        # already handles calling `downloader.close()` via `__exit__`,
        # unless sys.exit() is called earlier.
        # This `finally` block is now redundant for `close()` if using the `with` statement
        # but can stay for robustness if `downloader` somehow wasn't assigned before an error.
        if downloader and not downloader._is_closed:
             logger.debug("Ensuring downloader resources are explicitly closed (redundant check in finally block).")
             downloader.close()


if __name__ == "__main__":
    run_downloader_cli()