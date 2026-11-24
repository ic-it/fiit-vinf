"""Lazy native PyLucene initialization; ordinary CLI commands do not start Java."""

import warnings

from vinf_core.models import VinfError


def initialize():
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="builtin type .* has no __module__ attribute",
                category=DeprecationWarning,
            )
            import lucene
    except ImportError as exc:
        raise VinfError(
            "PyLucene is not installed for this Python. From the source checkout run: "
            "uv run python scripts/install_pylucene.py"
        ) from exc
    if lucene.getVMEnv() is None:
        lucene.initVM(vmargs=["-Djava.awt.headless=true", "-Xmx1g"])
    else:
        lucene.getVMEnv().attachCurrentThread()
    return lucene


def analyzer():
    initialize()
    from java.util import HashMap
    from org.apache.lucene.analysis.core import KeywordAnalyzer
    from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
    from org.apache.lucene.analysis.standard import StandardAnalyzer

    from vinf_lucene.schema import KEYWORDS

    exact = HashMap()
    for field in KEYWORDS:
        exact.put(field, KeywordAnalyzer())
    return PerFieldAnalyzerWrapper(StandardAnalyzer(), exact)
