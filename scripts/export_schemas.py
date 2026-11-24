"""Generate editor schemas for user-authored configuration and query files."""

import json
from pathlib import Path

from vinf_core.indexes import IndexConfig
from vinf_crawler.config import CrawlConfig
from vinf_search.models import QuerySet

from vinf.project import ProjectConfig

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    for path, model in [
        (ROOT / "config.schema.json", CrawlConfig),
        *(
            (ROOT / "schemas" / f"{model.__name__}.schema.json", model)
            for model in (ProjectConfig, IndexConfig, QuerySet)
        ),
    ]:
        path.write_text(
            json.dumps(model.model_json_schema(), indent=2) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    main()
