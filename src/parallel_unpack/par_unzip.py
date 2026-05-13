# %%
import zipfile
import tempfile
from functools import partial
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm
from datetime import datetime, timedelta
import subprocess
from loguru import logger
from pathlib import Path


def _chunk_generator(files, chunk_size):
    """Yields successive n-sized chunks from a list of files."""
    for i in range(0, len(files), chunk_size):
        yield files[i : i + chunk_size]


def unzip_file(zip_fname, target_fnames, target_dir):
    with zipfile.ZipFile(zip_fname, "r") as zip_ref:
        for target_fname in target_fnames:
            zip_ref.extract(target_fname, target_dir)


def parallel_unzip(
    zip_fname,
    target_dir,
    num_workers=8,
    max_chunk_size=1000,
    part_size=20_0000,
    executor_type="thread",
):
    """
    Unzips a zip file in parallel.

    Args:
        zip_fname (str or Path): Path to the zip file.
        target_dir (str or Path): Directory to extract files to.
        num_workers (int): Number of worker threads or processes.
        max_chunk_size (int): The maximum number of files in a chunk assigned to a worker.
                              The actual chunk size may be smaller to ensure all workers are utilized.
        part_size (int or None): Number of files to process in each part. If None, all files are processed in one go.
        executor_type (str): 'thread' for ThreadPoolExecutor or 'process' for ProcessPoolExecutor.
    """
    with zipfile.ZipFile(zip_fname, "r") as zip_ref:
        files = zip_ref.namelist()
    n_files = len(files)

    if part_size is None:
        parts = [files]
        n_parts = 1
        logger.info(f"Total files: {n_files}. Processing all in one part.")
    else:
        n_parts = (n_files + part_size - 1) // part_size
        parts = [
            files[i * part_size : min((i + 1) * part_size, n_files)]
            for i in range(n_parts)
        ]
        logger.info(
            f"Total files: {n_files}, Part size: {part_size}, Total parts: {n_parts}"
        )

    executor_class = (
        ThreadPoolExecutor if executor_type == "thread" else ProcessPoolExecutor
    )

    for i_part, files_part in enumerate(parts):
        if n_parts > 1:
            logger.info(f"Starting Part {i_part+1}/{n_parts} for {zip_fname}")

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
            target_dir_part = Path(target_dir) / f"part_{i_part+1}"
        else:
            target_dir_part = Path(target_dir)
        target_dir_part.mkdir(parents=True, exist_ok=True)
        unzip_one = partial(unzip_file, zip_fname, target_dir=target_dir_part)

        with executor_class(max_workers=num_workers) as executor:
            # Iterate over the map iterator directly without converting to a list
            for _ in tqdm(
                executor.map(unzip_one, file_chunks_gen),
                total=total_chunks,
                desc=f"{zip_fname} (part {i_part+1}, {chunk_size} files per chunk)",
            ):
                pass


# %%
