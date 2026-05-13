# parallel_unpack

[![PyPI version](https://badge.fury.io/py/parallel_unpack.svg)](https://badge.fury.io/py/parallel_unpack)

A simple and efficient tool to unpack `.zip` and `.tar.gz` archives in parallel, speeding up the extraction of large archives.

## Features

- **Fast Parallel Extraction**: Speeds up decompression using multiple threads or processes.
- **Broad Format Support**: Works with both `.zip` and `.tar.gz` archives.
- **Memory Efficient**: Handles massive archives by processing files in chunks.
- **Dual Interface**: Usable as both a Command-Line Tool and a Python Library.
- **User-Friendly**: Includes a progress bar, configurable workers, and optional logging.

## Installation

You can install the tool directly from PyPI:

```bash
pip install parallel_unpack
```

or via `uv`:

```bash
uv add parallel_unpack
```

## Quick Start

### As a Command-Line Tool

The package provides a simple `parallel_unpack` command.

**Basic Usage:**

```bash
parallel_unpack <ARCHIVE_FILE> <TARGET_DIRECTORY>
```

or via `uv`:

```bash
uv run parallel_unpack <ARCHIVE_FILE> <TARGET_DIRECTORY>
```

**Example:**

```bash
# Unpack a zip file using default settings (threads)
parallel_unpack my_archive.zip ./output_folder

# Unpack a tar.gz file using 8 processes
parallel_unpack data.tar.gz extracted_data --num-workers 8 --executor process

# Unpack a very large archive by splitting it into parts of 1,000,000 files each
parallel_unpack huge_archive.zip unpacked_huge_archive --part-size 1000000
```

For a full list of options, use the `--help` flag:

```bash
parallel_unpack --help
```

### As a Python Library

You can also import and use the `parallel_unpack` function in your own Python scripts.

```python
from parallel_unpack import parallel_unpack

# --- Basic Example ---
# Unpack a zip file with default settings
parallel_unpack(
    archive_file="my_archive.zip",
    target_dir="output_folder"
)

# --- Advanced Example ---
# Unpack a tar.gz file using 16 processes and splitting the file list into parts
parallel_unpack(
    archive_file="data.tar.gz",
    target_dir="extracted_data",
    num_workers=16,
    executor_type="process",
    part_size=500_000  # Process 500,000 files per major part
)

# --- Disable Logging ---
parallel_unpack(
    archive_file="archive.zip",
    target_dir="output",
    logger=None
)
```


## Documentation

### CLI help

For a full list of command-line options and arguments, you can use the `--help` flag. Here is the output:

**Usage:**

```bash
parallel_unpack [OPTIONS] ARCHIVE_FILE TARGET_DIR
```

**Description:**

Unpacks a `.zip` or `.tar.gz` archive in parallel using multiple workers.

**Arguments:**

*   `ARCHIVE_FILE`: Path to the archive file (`.zip` or `.tar.gz`) to unpack. (Required)
*   `TARGET_DIR`: Directory where the contents will be extracted. (Required)

**Options:**

*   `--num-workers, -w INTEGER`: Number of worker threads/processes. Defaults to the number of CPUs.
*   `--max-chunk-size INTEGER`: Maximum number of files per processing chunk. (Default: 1000)
*   `--part-size INTEGER`: Split the archive into parts of this size. Useful for very large archives.
*   `--executor, -e TEXT`: Executor type: `'thread'` or `'process'`. (Default: 'thread')
*   `--quiet, -q`: Disable logging.
*   `--install-completion`: Install completion for the current shell.
*   `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
*   `--help`: Show this message and exit.


### `parallel_unpack` function

The main function is `parallel_unpack`. Here is its full documentation:

**Signature:**
```python
def parallel_unpack(
    archive_file: Union[str, Path],
    target_dir: Union[str, Path],
    num_workers: int = -1,
    max_chunk_size: int = 1000,
    part_size: Optional[int] = None,
    executor_type: Literal["thread", "process"] = "thread",
    logger: Optional[object] = default_logger,
):
```

**Description:**

Unpacks a `.zip` or `.tar.gz` archive in parallel.

This function reads the list of files from the archive and distributes the extraction work across multiple workers using either threads or processes. It can split the extraction into larger 'parts' for very large archives and further divides the work within each part into smaller 'chunks' for each worker. A progress bar is displayed for monitoring.

**Arguments:**

*   `archive_file`: Path to the archive file (`.zip` or `.tar.gz`).
*   `target_dir`: Directory where the contents will be extracted.
*   `num_workers`: The number of worker threads or processes to use. If -1, it defaults to the number of CPUs on the system.
*   `max_chunk_size`: The maximum number of files to be processed in a single chunk by a worker.
*   `part_size`: An optional integer to split the archive's file list into larger parts. This is useful for managing very large archives. If `None` (default), the entire archive is processed as one part.
    > **NOTE:** It is only recommended for archives with a **flat or shallow directory** structure. If the archive file contains a deep directory structure, `part_size` should be `None` to ensure that all files in the same directory are processed together.
*   `executor_type`: The type of parallel executor to use. Can be `'thread'` for I/O-bound tasks or `'process'` for CPU-bound tasks.
*   `logger`: A custom logger object (e.g., from loguru or logging). If `None`, logging will be disabled. Defaults to a pre-configured loguru logger.

**Raises:**

*   `ValueError`: If the `archive_file` has an unsupported format.

**Examples:**

*   **Basic usage to unpack a zip file:**
    ```python
    >>> parallel_unpack("my_archive.zip", "output_folder")
    ```

*   **Unpack a tar.gz file using 16 processes:**
    ```python
    >>> parallel_unpack(
    ...     "data.tar.gz",
    ...     "extracted_data",
    ...     num_workers=16,
    ...     executor_type="process"
    ... )
    ```

*   **Unpack a very large archive by splitting it into parts of 1,000,000 files each:**
    ```python
    >>> parallel_unpack(
    ...     "huge_archive.zip",
    ...     "unpacked_huge_archive",
    ...     part_size=1_000_000
    ... )
    ```

*   **Unpack with logging disabled:**
    ```python
    >>> parallel_unpack("archive.zip", "output", logger=None)
    ```
