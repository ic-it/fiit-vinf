from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import chdir
from pathlib import Path

from typer.testing import CliRunner
from vinf_search.language import (
    AllOf,
    AnyOf,
    Kind,
    Negation,
    QuerySyntaxError,
    Text,
    lex,
    parse_query,
)

from vinf.cli.app import app
from vinf.project import PROJECT_FILE, load_project


class QueryAndProjectTests(unittest.TestCase):
    def test_query_precedence_escapes_and_syntax_errors(self):
        expression = parse_query('marine China OR "Alosa alosa" -reef')
        self.assertIsInstance(expression, AnyOf)
        self.assertTrue(all(isinstance(child, AllOf) for child in expression.children))
        self.assertIsInstance(expression.children[1].children[0], Text)
        self.assertTrue(expression.children[1].children[0].phrase)
        self.assertIsInstance(expression.children[1].children[1], Negation)
        tokens = lex(r'deep-water -reef "say \"hello\""')
        self.assertEqual(tokens[0].value, "deep-water")
        self.assertEqual(tokens[1].kind, Kind.MINUS)
        self.assertEqual(tokens[3].value, 'say "hello"')
        for query in (
            "",
            "marine OR",
            "AND marine",
            "(marine",
            "marine)",
            '"unclosed',
            '""',
            "length:<=15",
            "name:Alosa",
            "reef*",
            "reef~1",
            "marine AND OR reef",
            "(" * 33 + "marine" + ")" * 33,
        ):
            with (
                self.subTest(query=query),
                self.assertRaises(QuerySyntaxError) as caught,
            ):
                parse_query(query)
            self.assertIn("column", str(caught.exception))
            self.assertIn("^", str(caught.exception))

    def test_projects_discovery_overrides_and_independent_stages(self):
        runner = CliRunner()

        def invoke(*arguments, code=0):
            result = runner.invoke(app, list(arguments))
            self.assertEqual(
                result.exit_code, code, result.output + repr(result.exception)
            )
            return result.output

        with (
            tempfile.TemporaryDirectory() as temporary,
            chdir(temporary),
        ):
            root = Path.cwd()
            invoke(
                "project",
                "init",
                "--name",
                "pond",
                "--seed",
                "https://example.test/start",
            )
            project = load_project()
            self.assertEqual(project.config.name, "pond")
            invoke("crawl", "init")
            nested = root / "nested/working"
            nested.mkdir(parents=True)
            os.chdir(nested)
            self.assertEqual(load_project().root, root)
            status = json.loads(invoke("crawl", "status", "--json"))
            self.assertEqual(Path(status["dataset"]), root / "data/fishbase/crawled")
            parsed = json.loads(invoke("parse", "run", "--json"))
            self.assertEqual(parsed["documents"], 0)
            self.assertTrue((root / "data/fishbase/parsed/catalog.jsonl").is_file())
            self.assertFalse((root / "data/indexed").exists())
            other = root / "other-project"
            invoke(
                "project", "init", str(other), "--seed", "https://example.test/other"
            )
            invoke("--project", str(other), "crawl", "init")
            selected = json.loads(
                invoke("--project", str(other), "crawl", "status", "--json")
            )
            self.assertEqual(Path(selected["dataset"]), other / "data/fishbase/crawled")
            explicit = json.loads(
                invoke(
                    "--project",
                    str(other),
                    "crawl",
                    "status",
                    "--dataset",
                    str(root / "data/fishbase/crawled"),
                    "--json",
                )
            )
            self.assertEqual(Path(explicit["dataset"]), root / "data/fishbase/crawled")
            # A root option is scoped to one invocation, not leaked into later ones.
            self.assertEqual(
                Path(json.loads(invoke("crawl", "status", "--json"))["dataset"]),
                root / "data/fishbase/crawled",
            )
            invoke(
                "project",
                "add-source",
                "second",
                "--seed",
                "https://example.test/second",
            )
            self.assertIn("Choose --source", invoke("crawl", "status", code=1))
            invoke("crawl", "status", "--source", "fishbase")
            invalid = root / "bad-project"
            invoke(
                "project",
                "init",
                str(invalid),
                "--crawled",
                "data",
                "--parsed",
                "data/parsed",
                code=1,
            )
            self.assertFalse((invalid / PROJECT_FILE).exists())


if __name__ == "__main__":
    unittest.main()
