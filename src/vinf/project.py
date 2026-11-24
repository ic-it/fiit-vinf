"""File-based project configuration shared by independently invoked CLI stages."""

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from vinf_core.files import atomic_model, read_lock, read_model, separate_roots
from vinf_core.indexes import IndexConfig
from vinf_core.models import Model, VinfError
from vinf_crawler.config import CrawlConfig
from vinf_crawler.store import CrawlStore

PROJECT_FILE = "vinf.project.json"


class ProjectSource(Model):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    profile: str = "fishbase"
    crawled: Path
    parsed: Path
    crawl: CrawlConfig | None = None


class LucenePaths(Model):
    fishbase: Path = Path("data/spark/fishbase")
    linked: Path = Path("data/linked")
    indexed: Path = Path("data/lucene/indexed")


class ProjectConfig(Model):
    schema_version: Literal[1] = 1
    name: str = Field(min_length=1)
    sources: tuple[ProjectSource, ...] = Field(min_length=1)
    indexed: Path = Path("data/indexed")
    index: IndexConfig = IndexConfig()
    queries: Path | None = None
    lucene: LucenePaths = LucenePaths()

    @model_validator(mode="after")
    def unique_sources(self):
        if len({source.name for source in self.sources}) != len(self.sources):
            raise ValueError("Project source names must be unique.")
        return self


@dataclass(frozen=True)
class Project:
    root: Path
    config: ProjectConfig

    def resolve(self, path: Path) -> Path:
        return (self.root / path).resolve()

    def source(self, name: str | None = None) -> ProjectSource:
        if name is None and len(self.config.sources) == 1:
            return self.config.sources[0]
        for source in self.config.sources:
            if source.name == name:
                return source
        available = ", ".join(source.name for source in self.config.sources)
        raise VinfError(f"Choose --source from: {available}")

    def validate_paths(self) -> None:
        paths = [
            self.resolve(self.config.indexed),
            self.resolve(self.config.lucene.indexed),
        ]
        for source in self.config.sources:
            paths += [self.resolve(source.crawled), self.resolve(source.parsed)]
        if any(path.exists() and not path.is_dir() for path in paths):
            raise VinfError("Stage paths must point to directories.")
        for left, right in combinations(paths, 2):
            separate_roots(left, right)
        if any(self.root == path or self.root.is_relative_to(path) for path in paths):
            raise VinfError("Stage directories cannot contain the project root.")

    def save(self) -> None:
        self.validate_paths()
        atomic_model(self.root / PROJECT_FILE, self.config)


def load_project(path: Path | None = None) -> Project | None:
    if path is not None:
        file = path / PROJECT_FILE if path.is_dir() else path
        if not file.is_file():
            raise VinfError(f"Project configuration not found: {file}")
    else:
        current = Path.cwd()
        file = next(
            (
                parent / PROJECT_FILE
                for parent in (current, *current.parents)
                if (parent / PROJECT_FILE).is_file()
            ),
            None,
        )
        if file is None:
            return None
    project = Project(file.resolve().parent, read_model(file, ProjectConfig))
    project.validate_paths()
    return project


def make_source(
    root: Path,
    name: str,
    profile: str,
    crawled: Path | None = None,
    parsed: Path | None = None,
    seeds: tuple[str, ...] = (),
    crawl_config: Path | None = None,
) -> ProjectSource:
    crawled = crawled or Path("data") / name / "crawled"
    saved = root / crawled
    crawl = None
    if crawl_config:
        crawl = read_model(crawl_config, CrawlConfig)
    elif seeds:
        crawl = CrawlConfig(seeds=seeds)
    elif (saved / "crawl/state.json").is_file():
        with read_lock(saved / "crawl/lock"):
            crawl = CrawlStore(saved).config
    return ProjectSource(
        name=name,
        profile=profile,
        crawled=crawled,
        parsed=parsed or Path("data") / name / "parsed",
        crawl=crawl,
    )
