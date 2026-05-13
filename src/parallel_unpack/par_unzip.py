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


def unzip_file(zip_fname, target_fnames, target_dir):
    with zipfile.ZipFile(zip_fname, "r") as zip_ref:
        for target_fname in target_fnames:
            zip_ref.extract(target_fname, target_dir)


def parallel_unzip(
    zip_fname, target_dir, num_workers=8, chunk_size=100, epoch_size=20_000
):
    with zipfile.ZipFile(zip_fname, "r") as zip_ref:
        files = zip_ref.namelist()
    n_files = len(files)
    n_epochs = (n_files + epoch_size - 1) // epoch_size
    logger.info(
        f"Total files: {n_files}, Epoch size: {epoch_size}, Total epochs: {n_epochs}"
    )
    i_epoch = 0
    for i_epoch in range(n_epochs):
        logger.info(f"Starting Epoch {i_epoch+1}/{n_epochs} for {zip_fname}")
        files_epoch = files[
            i_epoch * epoch_size : min((i_epoch + 1) * epoch_size, n_files)
        ]
        max_chunk_size = len(files_epoch) // num_workers
        epoch_chunk_size = (
            min(chunk_size, max_chunk_size) if max_chunk_size > 0 else len(files_epoch)
        )

        file_chunks = [
            files[i : i + epoch_chunk_size]
            for i in range(0, len(files_epoch), epoch_chunk_size)
        ]
        n_chunked = len(file_chunks) * epoch_chunk_size
        if n_chunked < len(files_epoch):
            file_chunks.append(files_epoch[n_chunked:])

        target_dir_epoch = Path(target_dir) / f"epoch_{i_epoch+1}"
        target_dir_epoch.mkdir(parents=True, exist_ok=True)
        unzip_one = partial(unzip_file, zip_fname, target_dir=target_dir_epoch)

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # with ThreadPoolExecutor(max_workers=num_workers) as executor:
            list(
                tqdm(
                    executor.map(unzip_one, file_chunks),
                    total=len(file_chunks),
                    desc=f"{zip_fname} ({epoch_chunk_size} files per worker)",
                )
            )


# %%
