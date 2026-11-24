"""Explicit Spark sessions and publication of generated stage artifacts."""

import os
import shutil
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from vinf_core.files import artifact_lock, read_model
from vinf_core.models import VinfError

from vinf_spark.models import SparkSettings, StageReport


@contextmanager
def session(settings: SparkSettings):
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise VinfError("Install Spark support with: uv sync --extra spark") from exc
    if not os.environ.get("JAVA_HOME"):
        try:
            from jdk4py import JAVA_HOME
        except ImportError as exc:
            raise VinfError("Set JAVA_HOME or run: uv sync --extra spark") from exc
        os.environ["JAVA_HOME"] = str(JAVA_HOME)
    if settings.master.startswith("local"):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master(settings.master)
        .appName("FIIT VINF: REGEX extraction and Wikipedia linking")
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.shuffle.partitions", settings.partitions)
        .config("spark.default.parallelism", settings.partitions)
        .config("spark.ui.enabled", "false")
    )
    if settings.master.startswith("local"):
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


@contextmanager
def stage_output(output: Path, overwrite: bool):
    """Never remove arbitrary output directories or expose a failed new run."""
    with artifact_lock(output):
        existing = {path.name for path in output.iterdir()} - {".lock"}
        if existing:
            if not overwrite or "manifest.json" not in existing:
                raise VinfError(
                    f"Output already exists: {output}. Use --overwrite for a generated Spark stage."
                )
            previous = read_model(output / "manifest.json", StageReport)
            if not previous.producer.startswith("vinf-spark/"):
                raise VinfError("Refusing to replace another producer's output.")
        with tempfile.TemporaryDirectory(prefix=".building-", dir=output) as directory:
            work = Path(directory)
            yield work
            for name in existing:
                path = output / name
                shutil.rmtree(path) if path.is_dir() else path.unlink()
            for path in work.iterdir():
                os.replace(path, output / path.name)


def write_json(frame, output: Path, *, partitions: int, key: str, compression: str):
    frame.repartition(partitions, key).sortWithinPartitions(key).write.option(
        "compression", compression
    ).option("ignoreNullFields", "false").json(str(output))
