"""Boolean and exact phrase matching through positional postings."""

from collections import Counter, deque
from contextlib import ExitStack
from typing import Protocol

from vinf_core.index_io import IndexReader
from vinf_core.indexes import DocumentStats, IndexConfig, Occurrence, TermInfo
from vinf_core.models import VinfError
from vinf_core.postings import TermCursor
from vinf_core.text import canonical_text, occurrences, tokenize, words

from vinf_search.language import AllOf, AnyOf, Expression, Negation, Text
from vinf_search.models import MatchSpan


def prepare(node: Expression, config: IndexConfig) -> Expression | None:
    if isinstance(node, Text) and not node.phrase and not tokenize(node.value, config):
        return None
    if isinstance(node, Text) and not words(node.value):
        raise VinfError("A search word or phrase must contain letters or numbers.")
    if isinstance(node, (AllOf, AnyOf)):
        children = tuple(
            value
            for child in node.children
            if (value := prepare(child, config)) is not None
        )
        return type(node)(children=children) if children else None
    if isinstance(node, Negation):
        child = prepare(node.child, config)
        return Negation(child=child) if child is not None else None
    return node


def positive_terms(
    node: Expression,
    config: IndexConfig,
    *,
    negative: bool = False,
    include_phrases: bool = True,
) -> Counter[str]:
    if isinstance(node, Negation):
        return positive_terms(
            node.child, config, negative=not negative, include_phrases=include_phrases
        )
    if isinstance(node, (AllOf, AnyOf)):
        result = Counter()
        for child in node.children:
            result.update(
                positive_terms(
                    child, config, negative=negative, include_phrases=include_phrases
                )
            )
        return result
    if not negative and isinstance(node, Text) and (include_phrases or not node.phrase):
        return Counter(tokenize(node.value, config))
    return Counter()


class DocStream(Protocol):
    current: int | None

    def next(self) -> int | None: ...
    def advance_to(self, identifier: int) -> int | None: ...


class EmptyStream:
    current = None

    def next(self):
        return None

    def advance_to(self, identifier):
        return None


class TermStream:
    def __init__(self, cursor: TermCursor):
        self.cursor = cursor

    @property
    def current(self):
        return self.cursor.current.doc_id if self.cursor.current else None

    def next(self):
        self.cursor.next()
        return self.current

    def advance_to(self, identifier):
        self.cursor.advance_to(identifier)
        return self.current


class AndStream:
    def __init__(self, children: list[DocStream]):
        self.children = children
        self.current = None
        self._align()

    def _align(self):
        while all(child.current is not None for child in self.children):
            target = max(child.current for child in self.children)
            for child in self.children:
                child.advance_to(target)
            if all(child.current == target for child in self.children):
                self.current = target
                return self.current
        self.current = None
        return None

    def next(self):
        self.children[0].next()
        return self._align()

    def advance_to(self, identifier):
        if self.current is None or self.current >= identifier:
            return self.current
        for child in self.children:
            child.advance_to(identifier)
        return self._align()


class OrStream:
    def __init__(self, children: list[DocStream]):
        self.children = children
        self._align()

    def _align(self):
        self.current = min(
            (child.current for child in self.children if child.current is not None),
            default=None,
        )
        return self.current

    def next(self):
        for child in self.children:
            if child.current == self.current:
                child.next()
        return self._align()

    def advance_to(self, identifier):
        if self.current is None or self.current >= identifier:
            return self.current
        for child in self.children:
            child.advance_to(identifier)
        return self._align()


class NotStream:
    def __init__(self, child: DocStream, count: int):
        self.child, self.count, self.current = child, count, 0
        self._align()

    def _align(self):
        while self.current is not None and self.current < self.count:
            self.child.advance_to(self.current)
            if self.child.current != self.current:
                return self.current
            self.current += 1
        self.current = None
        return None

    def next(self):
        if self.current is not None:
            self.current += 1
        return self._align()

    def advance_to(self, identifier):
        if self.current is not None:
            self.current = max(self.current, identifier)
        return self._align()


class PhraseStream:
    def __init__(
        self,
        reader: IndexReader,
        terms: tuple[str, ...],
        query: str,
        streams: dict[str, TermStream],
        *,
        capture: bool = False,
    ):
        self.reader, self.terms, self.query, self.streams = (
            reader,
            terms,
            query,
            streams,
        )
        self.capture = capture
        self.matches: list[MatchSpan] = []
        spans = tuple(occurrences(query))
        self.leading = spans[0][1].start
        self.trailing = len(query.encode()) - spans[-1][1].end
        self.scan_text = any(term not in streams for term in terms)
        self.base = (
            AndStream(list(streams.values()))
            if streams
            else NotStream(EmptyStream(), reader.manifest.documents)
        )
        self._align()

    @property
    def current(self):
        return self.base.current

    def _matches(self) -> bool:
        self.matches = []
        document = self.reader.stats(self.current)
        if self.scan_text:
            # Missing stop-word postings cannot verify the complete phrase.
            window = deque(maxlen=len(self.terms))
            for item in occurrences(self.reader.text(document)):
                window.append(item)
                if len(window) == len(self.terms) and all(
                    expected == actual
                    for expected, (actual, _) in zip(self.terms, window)
                ):
                    span = self._match_span(document, window[0][1], window[-1][1])
                    if span is not None:
                        self.matches.append(span)
                        if not self.capture:
                            return True
            return bool(self.matches)
        with ExitStack() as stack:
            readers = [
                stack.enter_context(self.streams[term].cursor.positions())
                for term in self.terms
            ]
            values = [next(iterator, None) for iterator in readers]
            while all(value is not None for value in values):
                target = max(
                    value.position - index for index, value in enumerate(values)
                )
                for index, iterator in enumerate(readers):
                    while (
                        values[index] is not None
                        and values[index].position - index < target
                    ):
                        values[index] = next(iterator, None)
                if any(value is None for value in values):
                    return bool(self.matches)
                if any(
                    value.position - index != target
                    for index, value in enumerate(values)
                ):
                    continue
                first, last = values[0], values[-1]
                span = self._match_span(document, first, last)
                if span is not None:
                    self.matches.append(span)
                    if not self.capture:
                        return True
                values[0] = next(readers[0], None)
        return bool(self.matches)

    def _match_span(
        self, document: DocumentStats, first: Occurrence, last: Occurrence
    ) -> MatchSpan | None:
        start, end = first.start - self.leading, last.end + self.trailing
        if first.segment != last.segment or not 0 <= start <= end <= document.text_size:
            return None
        fragment = self.reader.text_span(document, start, end)
        try:
            matches = (
                b"\n" not in fragment
                and canonical_text(fragment.decode()).casefold()
                == self.query.casefold()
            )
        except UnicodeError:
            return None
        if not matches:
            return None
        return MatchSpan(
            kind="phrase",
            value=self.query,
            start=start,
            end=end,
            position=first.position,
            segment=first.segment,
        )

    def _align(self):
        while self.base.current is not None and not self._matches():
            self.base.next()
        return self.current

    def next(self):
        self.base.next()
        return self._align()

    def advance_to(self, identifier):
        if self.current is None or self.current >= identifier:
            return self.current
        self.base.advance_to(identifier)
        return self._align()


class Matcher:
    """Query-sized metadata cache and cursor ownership; no corpus-sized sets."""

    def __init__(self, reader: IndexReader, stack: ExitStack, *, capture: bool = False):
        self.reader, self.stack = reader, stack
        self.config = reader.manifest.config
        self.infos: dict[str, TermInfo | None] = {}
        self.capture = capture
        self.phrases: list[PhraseStream] = []

    def phrase_matches(self, identifier: int) -> tuple[MatchSpan, ...]:
        return tuple(
            span
            for phrase in self.phrases
            if phrase.current == identifier
            for span in phrase.matches
        )

    def info(self, term: str) -> TermInfo | None:
        if term not in self.infos:
            self.infos[term] = self.reader.find(term)
        return self.infos[term]

    def term_stream(self, term: str) -> TermStream | EmptyStream:
        info = self.info(term)
        if info is None:
            return EmptyStream()
        return TermStream(self.stack.enter_context(self.reader.cursor(info)))

    def evaluate(self, node: Expression, *, negative: bool = False) -> DocStream:
        if isinstance(node, (AllOf, AnyOf)):
            children = [
                self.evaluate(child, negative=negative) for child in node.children
            ]
            return (
                AndStream(children) if isinstance(node, AllOf) else OrStream(children)
            )
        if isinstance(node, Negation):
            return NotStream(
                self.evaluate(node.child, negative=not negative),
                self.reader.manifest.documents,
            )
        terms = tokenize(node.value, self.config)
        streams = {term: self.term_stream(term) for term in dict.fromkeys(terms)}
        if node.phrase:
            if any(isinstance(stream, EmptyStream) for stream in streams.values()):
                return EmptyStream()
            phrase = PhraseStream(
                self.reader,
                words(node.value),
                canonical_text(node.value),
                streams,
                capture=self.capture and not negative,
            )
            if self.capture and not negative:
                self.phrases.append(phrase)
            return phrase
        return AndStream(list(streams.values()))
