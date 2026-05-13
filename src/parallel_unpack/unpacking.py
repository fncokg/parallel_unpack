import zipfile
import tarfile
import os

from typing import Union, Optional, Literal
from functools import partial
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm

from loguru import logger as default_logger
from pathlib import Path


def _chunk_generator(files, chunk_size):
    """Yields successive n-sized chunks from a list of files."""
    for i in range(0, len(files), chunk_size):
        yield files[i : i + chunk_size]


def _unpack_zip_reader(archive_file):
    with zipfile.ZipFile(archive_file, "r") as zip_ref:
        return zip_ref.namelist()


def _unpack_tar_gz_reader(archive_file):
    with tarfile.open(archive_file, "r:gz") as tar_ref:
        return tar_ref.getnames()


def _unpack_zip_worker(archive_file, target_fnames, target_dir):
    with zipfile.ZipFile(archive_file, "r") as zip_ref:
        for target_fname in target_fnames:
            zip_ref.extract(target_fname, target_dir)


def _unpack_tar_gz_worker(archive_file, target_fnames, target_dir):
    with tarfile.open(archive_file, "r:gz") as tar_ref:
        for target_fname in target_fnames:
            tar_ref.extract(target_fname, target_dir)


def parallel_unpack(
    archive_file: Union[str, Path],
    target_dir: Union[str, Path],
    num_workers: int = -1,
    max_chunk_size: int = 1000,
    part_size: Optional[int] = None,
    executor_type: Literal["thread", "process"] = "thread",
    logger: Optional[object] = default_logger,
):
    """Unpacks a .zip or .tar.gz archive in parallel.

    This function reads the list of files from the archive and distributes the
    extraction work across multiple workers using either threads or processes.
    It can split the extraction into larger 'parts' for very large archives
    and further divides the work within each part into smaller 'chunks' for
    each worker. A progress bar is displayed for monitoring.

    Args:
        archive_file: Path to the archive file (.zip or .tar.gz).
        target_dir: Directory where the contents will be extracted.
        num_workers: The number of worker threads or processes to use.
            If -1, it defaults to the number of CPUs on the system.
        max_chunk_size: The maximum number of files to be processed in a single
            chunk by a worker.
        part_size: An optional integer to split the archive's file list into
            larger parts. This is useful for managing very large archives. If None (default), the entire archive is processed as one part.
            NOTE: It is only recommended for archives with a **flat or shallow directory**
            structure. If the archive file contains a deep directory structure, `part_size` should be `None` to ensure that all files in the same directory are processed together.
        executor_type: The type of parallel executor to use. Can be 'thread'
            for I/O-bound tasks or 'process' for CPU-bound tasks.
        logger: A custom logger object (e.g., from loguru or logging).
            If None, logging will be disabled. Defaults to a pre-configured
            loguru logger.

    Raises:
        ValueError: If the archive_file has an unsupported format.

    Examples:
        >>> # Basic usage to unpack a zip file
        >>> parallel_unpack("my_archive.zip", "output_folder")

        >>> # Unpack a tar.gz file using 16 processes
        >>> parallel_unpack(
        ...     "data.tar.gz",
        ...     "extracted_data",
        ...     num_workers=16,
        ...     executor_type="process"
        ... )

        >>> # Unpack a very large archive by splitting it into parts of 1,000,000 files each
        >>> parallel_unpack(
        ...     "huge_archive.zip",
        ...     "unpacked_huge_archive",
        ...     part_size=1_000_000
        ... )

        >>> # Unpack with logging disabled
        >>> parallel_unpack("archive.zip", "output", logger=None)
    """
    log = logger.info if logger else lambda *args, **kwargs: None

    num_workers = os.cpu_count() if num_workers == -1 else num_workers
    archive_file = (
        Path(archive_file) if not isinstance(archive_file, Path) else archive_file
    )
    target_dir = Path(target_dir) if not isinstance(target_dir, Path) else target_dir

    if archive_file.name.endswith(".zip"):
        reader_func = _unpack_zip_reader
        worker_func = _unpack_zip_worker
    elif archive_file.name.endswith(".tar.gz"):
        reader_func = _unpack_tar_gz_reader
        worker_func = _unpack_tar_gz_worker
    else:
        raise ValueError(f"Unsupported archive format: {archive_file}")

    files = reader_func(archive_file)
    n_files = len(files)

    # Simplified Part Logic
    effective_part_size = (
        part_size if part_size is not None and part_size > 0 else n_files
    )
    n_parts = (n_files + effective_part_size - 1) // effective_part_size
    parts_gen = _chunk_generator(files, effective_part_size)

    if part_size is not None and part_size > 0:
        log(
            f"Total files: {n_files}, Part size: {effective_part_size}, Total parts: {n_parts}"
        )
    else:
        log(f"Total files: {n_files}. Processing all in one part.")

    executor_class = (
        ThreadPoolExecutor if executor_type == "thread" else ProcessPoolExecutor
    )

    for i_part, files_part in enumerate(parts_gen):
        if n_parts > 1:
            log(f"Starting Part {i_part+1}/{n_parts} for {archive_file}")

        # Adjust chunk_size to ensure every worker gets at least one chunk
        num_chunks_min = num_workers
        if len(files_part) < num_chunks_min:
            chunk_size = 1
        else:
            # Calculate chunk size to distribute work evenly, but not exceeding max_chunk_size
            chunks_per_worker = (len(files_part) + num_workers - 1) // num_workers
            chunk_size = min(max_chunk_size, chunks_per_worker)

        if chunk_size == 0:
            chunk_size = 1

        # Use a generator for chunking to save memory
        file_chunks_gen = _chunk_generator(files_part, chunk_size)
        total_chunks = (len(files_part) + chunk_size - 1) // chunk_size

        if n_parts > 1:
            target_dir_part = target_dir / f"part_{i_part+1}"
        else:
            target_dir_part = target_dir
        target_dir_part.mkdir(parents=True, exist_ok=True)
        unpack_worker = partial(
            worker_func, archive_file=archive_file, target_dir=target_dir_part
        )

        desc = (
            f"{archive_file} (part {i_part+1}/{n_parts}, {chunk_size} files per chunk)"
            if n_parts > 1
            else f"{archive_file} (all files, {chunk_size} files per chunk)"
        )
        with executor_class(max_workers=num_workers) as executor:
            # Iterate over the map iterator directly without converting to a list
            for _ in tqdm(
                executor.map(unpack_worker, file_chunks_gen),
                total=total_chunks,
                desc=desc,
            ):
                pass
