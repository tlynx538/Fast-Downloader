# fast-downloader

`fast-downloader` is a command-line utility for efficiently downloading large files using multi-threaded connections.  
It supports resuming interrupted downloads and includes optional SHA256 hash verification to ensure file integrity.  

**Author:** [tlynx538](https://github.com/tlynx538)

---

## Features

- **Multi-threaded Downloads**  
  Splits files into chunks and downloads them concurrently for faster performance.

- **Resumable Downloads**  
  Automatically detects and resumes interrupted downloads, continuing from where it left off.

- **Integrity Verification**  
  Supports SHA256 hash verification to ensure the downloaded file is not corrupted.

- **Real-time Progress Display**  
  Provides a dynamic command-line interface showing overall progress and active download threads.

- **Single-threaded Fallback**  
  Gracefully falls back to single-threaded download if the server does not support range requests.

---

## Installation

### Using [uv](https://github.com/astral-sh/uv) (Recommended)

First, install `uv` if you don't have it:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then, install dependencies:

```bash
uv sync
```

---

## Building the Package

You can build the package using `uv`:

```bash
uv build
```

This will create a distribution under the `dist/` directory.

---

## Usage

```bash
python3 main.py <URL> [OPTIONS]
```

**Positional Argument:**

- `<URL>`  
  The URL of the file to download. (Required)

**Optional Arguments:**

- `-o, --output <DIRECTORY>`  
  Output directory where the downloaded file will be saved.  
  Default: Current working directory (.)

- `-f, --force`  
  Overwrite the file if it already exists in the output directory.  
  Default: False

- `-v, --verbose`  
  Enable verbose logging (shows thread-level and chunk-level debug info).  
  Default: False

- `--sha256 <HASH>`  
  Expected SHA256 hash of the file. After download, the file’s hash will be verified against this.  
  Default: None (no verification)

- `--logs`  
  Enable writing logs to a file inside a logs/ directory (created next to main.py).  
  Default: False

---

## Examples

### Basic Download

```bash
python3 main.py https://example.com/large_file.zip
```

### Download to a Specific Directory

```bash
python3 main.py https://example.com/file.tar.gz -o /path/to/downloads
```

### Download with SHA256 Verification

```bash
python3 main.py https://releases.ubuntu.com/24.04.2/ubuntu-24.04.2-desktop-amd64.iso \
  --sha256 d7fe3d6a0419667d2f8eff12796996328daa2d4f90cd9f87aa9371b362f987bf
```

### Resume an Interrupted Download

```bash
python3 main.py https://releases.ubuntu.com/24.04.2/ubuntu-24.04.2-desktop-amd64.iso
```
The downloader automatically detects incomplete chunks and resumes only the missing parts.

### Overwrite Existing File with Verbose Output and Logging to File

```bash
python3 main.py https://example.com/file.iso -f