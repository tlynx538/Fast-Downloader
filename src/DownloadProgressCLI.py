# file: src/DownloadProgressCLI
# author: tlynx538 
# author: tlynx538 (Vinayak Jaiwant Mooliyil)
import threading
import time
from queue import Queue
from typing import Set, Dict, Any, Optional
import logging

from rich.live import Live
from rich.table import Table
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn, TaskID
from rich.console import Group, Console 

from . import constants
# CORRECTED IMPORT: Import DownloaderUtils.format_bytes directly as it's likely a standalone function
from .DownloaderUtils import DownloaderUtils

logger = logging.getLogger(__name__)

class DownloadProgressCLI:
    """
    Provides an interactive command-line interface for displaying download progress.
    It observes the FileDownloader's state without modifying the FileDownloader itself.
    """
    def __init__(self, downloader_instance: Any):
        self.downloader = downloader_instance
        self.stop_event = threading.Event()

        self.overall_progress_bar: Optional[Progress] = None
        self.overall_task: Optional[TaskID] = None
        
        # Dictionary to hold current progress for active chunks (no rich.Progress for these)
        # Format: {'partX': {'current': Y_bytes, 'total': Z_bytes}}
        self.active_chunk_states: Dict[str, Dict[str, Any]] = {} 

        self.all_chunk_ids: Set[str] = set() # For CLI's internal tracking of all expected parts
        self.completed_chunk_ids: Set[str] = set() # For tracking chunks completed during CLI lifecycle

        self.start_time = time.time()
        self.display_thread: Optional[threading.Thread] = None
        self.final_download_duration: float = 0.0

        # These attributes are expected to be available from the downloader_instance
        required_attrs = ['file_name', 'file_headers', 'total_chunk_length', 
                          'chunk_tracker', 'completed_on_resume', 
                          '_single_threaded_current_bytes', 'active_chunk_progress_updates'] 
        for attr in required_attrs:
            if not hasattr(self.downloader, attr):
                logger.critical(f"FileDownloader instance is missing required attribute for CLI: '{attr}'. Cannot start CLI.")
                raise AttributeError(f"FileDownloader instance is missing required attribute: '{attr}'")

        # Initialize the overall progress bar
        self.overall_progress_bar = Progress(
            TextColumn("[bold green]Downloading [cyan]{task.fields[filename]}[/]", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:.0f}%", 
            "•",
            TextColumn("Chunks: [green]{task.fields[chunks_completed]}/{task.fields[chunks_total]}[/]", justify="left"),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            transient=True # Hide when done
        )

        self.console = Console() # Initialize Rich Console for direct printing


    def _get_overall_progress_data(self) -> Dict[str, Any]:
        """Calculates current overall progress metrics."""
        total_chunks = self.downloader.total_chunk_length
        
        # Count completed chunks from the downloader's initial scan and what the CLI has seen complete
        completed_chunks_count = len(self.downloader.completed_on_resume) + len(self.completed_chunk_ids)

        total_file_size_str = self.downloader.file_headers.get('Content-Length')
        total_file_size = int(total_file_size_str) if total_file_size_str else 0

        # Calculate current downloaded bytes from all active_chunk_states + already completed chunks
        current_downloaded_bytes = 0
        
        # Add bytes from currently active chunks
        for part_id, state in self.active_chunk_states.items():
            current_downloaded_bytes += state.get('current', 0)

        # Add bytes from chunks completed on resume and those completed during this session
        # This is an approximation as actual chunk sizes can vary slightly, but good enough for overall progress
        if total_file_size > 0 and total_chunks > 0:
            approx_chunk_size = total_file_size / total_chunks
            # Sum bytes for chunks that are in completed_on_resume but are no longer active
            # This ensures we count resumed chunks towards overall progress only once.
            for part_id in self.downloader.completed_on_resume:
                if part_id not in self.active_chunk_states and part_id not in self.completed_chunk_ids: 
                    current_downloaded_bytes += approx_chunk_size
            
            # Sum bytes for chunks completed *during* this session
            for part_id in self.completed_chunk_ids:
                if part_id not in self.active_chunk_states: # Ensure not double counted if still processing
                    current_downloaded_bytes += approx_chunk_size

        # Special handling for single-threaded fallback
        if self.downloader.file_headers.get('Accept-Ranges') != 'bytes' and total_file_size > 0:
            current_downloaded_bytes = self.downloader._single_threaded_current_bytes
            total_chunks = 1 # Force overall display to 1 chunk for single-threaded
            completed_chunks_count = 1 if current_downloaded_bytes >= total_file_size and total_file_size > 0 else 0


        elapsed_time = time.time() - self.start_time
        
        return {
            "total_chunks": total_chunks,
            "completed_chunks": completed_chunks_count,
            "total_file_size": total_file_size,
            "current_downloaded_bytes": current_downloaded_bytes,
            "elapsed_time": elapsed_time
        }


    def _display_loop(self):
        """
        The main loop for updating and displaying progress in the CLI.
        Runs in a separate thread.
        """
        # --- Initial overall task setup is now done here, inside the Live context ---
        # This ensures it's only set up once Live takes over.
        
        # Get initial progress data, especially for total file size and total chunks
        initial_progress_data = {
            "total_file_size": int(self.downloader.file_headers.get('Content-Length', '0')),
            "total_chunks": self.downloader.total_chunk_length if self.downloader.file_headers.get('Accept-Ranges') == 'bytes' else 1,
            "filename": self.downloader.file_name,
            "current_downloaded_bytes": 0, # Start at 0 for display
            "completed_chunks": 0 # Start at 0 for display
        }
        
        self.overall_task = self.overall_progress_bar.add_task(
            "", 
            total=initial_progress_data["total_file_size"],
            completed=initial_progress_data["current_downloaded_bytes"],
            filename=initial_progress_data["filename"],
            chunks_completed=initial_progress_data["completed_chunks"],
            chunks_total=initial_progress_data["total_chunks"]
        )

        # Start with just the overall bar
        live_group = Group(self.overall_progress_bar) 

        with Live(live_group, refresh_per_second=constants._CLI_REFRESH_RATE, screen=False) as live:
            while not self.stop_event.is_set():
                # Process completed chunks from chunk_tracker (for overall count)
                while not self.downloader.chunk_tracker.empty():
                    try:
                        completed_part_id = self.downloader.chunk_tracker.get_nowait()
                        if completed_part_id not in self.completed_chunk_ids:
                            self.completed_chunk_ids.add(completed_part_id)
                            logger.debug(f"CLI: Overall count marked chunk {completed_part_id} as completed.")
                        self.downloader.chunk_tracker.task_done()
                    except Exception as e:
                        logger.debug(f"CLI: Error getting from chunk_tracker (likely empty): {e}")
                        pass

                # Process active chunk progress updates
                while not self.downloader.active_chunk_progress_updates.empty():
                    try:
                        part_id, current_bytes, total_bytes = self.downloader.active_chunk_progress_updates.get_nowait()
                        if current_bytes == -1 and total_bytes == -1: # Sentinel for completion/removal
                            if part_id in self.active_chunk_states:
                                del self.active_chunk_states[part_id]
                                logger.debug(f"CLI: Removed chunk {part_id} from active states.")
                        else:
                            self.active_chunk_states[part_id] = {'current': current_bytes, 'total': total_bytes}
                        self.downloader.active_chunk_progress_updates.task_done()
                    except Exception as e:
                        logger.debug(f"CLI: Error getting from active_chunk_progress_updates (likely empty): {e}")
                        pass

                progress_data = self._get_overall_progress_data()
                
                # Update overall progress
                self.overall_progress_bar.update(
                    self.overall_task,
                    completed=progress_data["current_downloaded_bytes"],
                    chunks_completed=progress_data["completed_chunks"], 
                    chunks_total=progress_data["total_chunks"]
                )

                # Create and update the active chunks table
                active_chunks_table = Table(
                    title="[bold blue]Active Threads (Chunks)[/bold blue]",
                    header_style="bold magenta",
                    show_footer=False,
                    box=None, # No box for a cleaner look
                    show_lines=False # No internal lines
                )
                active_chunks_table.add_column("Chunk ID", style="cyan", justify="left")
                active_chunks_table.add_column("Progress (Bytes)", style="green", justify="right")
                active_chunks_table.add_column("Progress (%)", style="yellow", justify="right")

                if self.active_chunk_states:
                    # Sort active chunks by ID for consistent display
                    sorted_active_chunks = sorted(self.active_chunk_states.items(), key=lambda item: int(item[0].replace('part', '')))
                    for part_id, state in sorted_active_chunks:
                        current = state['current']
                        total = state['total']
                        
                        progress_percentage = (current / total * 100) if total > 0 else 0
                        
                        active_chunks_table.add_row(
                            str(part_id.replace('part', '')), # Just the ID number
                            # Use the directly imported DownloaderUtils.format_bytes function
                            f"{DownloaderUtils.format_bytes(current)} / {DownloaderUtils.format_bytes(total)}", 
                            f"{progress_percentage:.1f}%"
                        )
                else:
                    # Display a message if no chunks are active (e.g., download just started or finished)
                    active_chunks_table.add_row("[dim]No active threads currently downloading chunks.[/dim]", " ", " ")


                # Group for Live display: overall bar, then active chunks table
                # The table is always there, but its content changes
                live.update(Group(self.overall_progress_bar, active_chunks_table))

                time.sleep(1 / constants._CLI_REFRESH_RATE)

            # --- Final state update and message ---
            self.final_download_duration = time.time() - self.start_time
            
            progress_data = self._get_overall_progress_data()
            self.overall_progress_bar.update(
                self.overall_task,
                completed=progress_data["total_file_size"], # Ensure 100% completed bytes
                chunks_completed=progress_data["total_chunks"], # Ensure 100% completed chunks
                total=progress_data["total_file_size"] # Final total
            )
            
            final_message_table = Table.grid(expand=True)
            final_message_table.add_column(justify="center", style="bold green")
            final_message_table.add_row(f"Download Complete! Total time: {self.final_download_duration:.2f}s")

            # Final live update with just the overall bar and completion message
            live.update(Group(self.overall_progress_bar, final_message_table))

            time.sleep(1.0) # Allow final message to be displayed for a moment

        logger.debug("CLI display loop finished.")


    def start(self):
        """Starts the CLI progress display in a separate daemon thread."""
        logger.debug("Starting CLI display thread.")
        self.display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self.display_thread.start()

    def stop(self):
        """Signals the CLI progress display thread to stop and waits for it to finish."""
        logger.debug("Stopping CLI display thread.")
        self.stop_event.set()
        if self.display_thread and self.display_thread.is_alive():
            self.display_thread.join(timeout=constants._CLI_STOP_TIMEOUT_SECONDS)
            if self.display_thread.is_alive():
                logger.warning("CLI progress display thread did not stop gracefully. It may be stuck.")
            else:
                logger.debug("CLI progress display thread stopped successfully.")
        else:
            logger.debug("CLI display thread not active or already stopped.")