"""Package current sources and documentation. Never publish or include user data."""

from pathlib import Path
from typing import Annotated
from zipfile import ZIP_DEFLATED, ZipFile

import typer
from rich.console import Console
from vinf_core.files import file_hash

from vinf import __version__

PROJECT = Path(__file__).resolve().parents[1]
ROOT_FILES = (
    "pyproject.toml",
    "uv.lock",
    ".python-version",
    ".gitignore",
    "README.md",
    "config.example.json",
    "proxies.example.json",
    "config.schema.json",
    "index.example.json",
    "queries.example.json",
    "project.example.json",
)
DIRECTORIES = ("packages", "src", "tests", "scripts", "schemas", "wiki")
SUFFIXES = {".py", ".toml", ".json", ".txt", ".svg", ".png", ".md", ".zip", ".xml"}


def package(output: Path) -> Path:
    files = [PROJECT / name for name in ROOT_FILES]
    files += [
        path
        for directory in DIRECTORIES
        for path in (PROJECT / directory).rglob("*")
        if path.is_file()
        and path.suffix in SUFFIXES
        and not {"__pycache__", ".venv", "dist", ".ruff_cache"}.intersection(
            path.relative_to(PROJECT).parts
        )
    ]
    output = output.resolve()
    if output in files:
        raise ValueError("The archive destination cannot replace a source file.")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".zip.tmp")
    try:
        with ZipFile(
            temporary, "w", compression=ZIP_DEFLATED, compresslevel=9
        ) as archive:
            for path in sorted(files):
                archive.write(path, path.relative_to(PROJECT))
        with ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise ValueError("Archive verification failed.")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def main(
    output: Annotated[Path, typer.Option()] = Path(
        f"dist/vinf-source-{__version__}.zip"
    ),
):
    path = package(output)
    Console().print(
        f"{path}\nBytes: {path.stat().st_size}\nSHA-256: {file_hash(path)}",
        markup=False,
    )


if __name__ == "__main__":
    typer.run(main)
