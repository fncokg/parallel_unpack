import typer
from pathlib import Path
from typing import Optional, Annotated

from .unpacking import parallel_unpack, default_logger as logger

app = typer.Typer()


@app.command()
def main(
    archive_file: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to the archive file (.zip or .tar.gz) to unpack.",
        ),
    ],
    target_dir: Annotated[
        Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
            help="Directory where the contents will be extracted.",
        ),
    ],
    num_workers: Annotated[
        int,
        typer.Option(
            "-w",
            "--num-workers",
            help="Number of worker threads/processes. Defaults to the number of CPUs.",
        ),
    ] = -1,
    max_chunk_size: Annotated[
        int,
        typer.Option(
            "--max-chunk-size", help="Maximum number of files per processing chunk."
        ),
    ] = 1000,
    part_size: Annotated[
        Optional[int],
        typer.Option(
            "--part-size",
            help="Split the archive into parts of this size. Useful for very large archives.",
        ),
    ] = None,
    executor_type: Annotated[
        str,
        typer.Option(
            "-e",
            "--executor",
            help="Executor type: 'thread' or 'process'.",
        ),
    ] = "thread",
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Disable logging.",
        ),
    ] = False,
):
    """
    Unpacks a .zip or .tar.gz archive in parallel using multiple workers.
    """
    custom_logger = logger if not quiet else None
    if custom_logger:
        custom_logger.add(
            lambda msg: print(msg, end=""),
            format="{message}",
            level="INFO",
            colorize=True,
        )

    try:
        parallel_unpack(
            archive_file=archive_file,
            target_dir=target_dir,
            num_workers=num_workers,
            max_chunk_size=max_chunk_size,
            part_size=part_size,
            executor_type=executor_type,
            logger=custom_logger,
        )
        print(f"\n✅ Successfully unpacked '{archive_file.name}' to '{target_dir}'")
    except Exception as e:
        print(f"\n❌ Error unpacking file: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
