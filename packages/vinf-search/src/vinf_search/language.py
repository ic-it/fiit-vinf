"""A small Google-style query language: typed AST and recursive-descent parser."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from vinf_core.models import Model, VinfError


class QuerySyntaxError(VinfError):
    def __init__(self, query: str, position: int, message: str):
        super().__init__(
            f"{message} (column {position + 1})\n{query}\n{' ' * position}^"
        )


class Kind(StrEnum):
    WORD = "word"
    PHRASE = "phrase"
    LPAREN = "("
    RPAREN = ")"
    MINUS = "-"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    END = "end"


class Token(Model):
    kind: Kind
    value: str
    position: int


def lex(query: str) -> tuple[Token, ...]:
    if len(query) > 4096:
        raise QuerySyntaxError(query[:80], 0, "Query exceeds 4096 characters")
    tokens = []
    position = 0
    punctuation = {"(": Kind.LPAREN, ")": Kind.RPAREN, "-": Kind.MINUS}
    while position < len(query):
        if query[position].isspace():
            position += 1
            continue
        start, char = position, query[position]
        if char == ":":
            raise QuerySyntaxError(
                query,
                start,
                "Field filters are not supported; use words or quoted phrases",
            )
        if char in punctuation:
            tokens.append(Token(kind=punctuation[char], value=char, position=start))
            position += 1
        elif char == '"':
            position += 1
            value = []
            while position < len(query) and query[position] != '"':
                if query[position] == "\\":
                    position += 1
                    if position == len(query) or query[position] not in {'"', "\\"}:
                        raise QuerySyntaxError(
                            query, position - 1, "Escape only quotes or backslashes"
                        )
                value.append(query[position])
                position += 1
            if position == len(query):
                raise QuerySyntaxError(query, start, "Unclosed quoted phrase")
            if not "".join(value).strip():
                raise QuerySyntaxError(query, start, "A quoted phrase cannot be empty")
            tokens.append(Token(kind=Kind.PHRASE, value="".join(value), position=start))
            position += 1
        else:
            while (
                position < len(query)
                and not query[position].isspace()
                and query[position] not in '():"'
            ):
                position += 1
            value = query[start:position]
            if "*" in value or "~" in value:
                raise QuerySyntaxError(
                    query, start, "Wildcard and fuzzy operators are not supported"
                )
            kind = Kind(value) if value in {"AND", "OR", "NOT"} else Kind.WORD
            tokens.append(Token(kind=kind, value=value, position=start))
        if len(tokens) > 256:
            raise QuerySyntaxError(query, start, "Query exceeds 256 tokens")
    return (*tokens, Token(kind=Kind.END, value="", position=len(query)))


class Text(Model):
    kind: Literal["text"] = "text"
    value: str
    phrase: bool = False


class Negation(Model):
    kind: Literal["not"] = "not"
    child: Expression


class AllOf(Model):
    kind: Literal["and"] = "and"
    children: tuple[Expression, ...]


class AnyOf(Model):
    kind: Literal["or"] = "or"
    children: tuple[Expression, ...]


type Expression = Text | Negation | AllOf | AnyOf
for _model in (Negation, AllOf, AnyOf):
    _model.model_rebuild()


class Parser:
    def __init__(self, query: str):
        self.query, self.tokens, self.position = query, lex(query), 0
        self.depth = 0

    @property
    def token(self) -> Token:
        return self.tokens[self.position]

    def take(self) -> Token:
        token = self.token
        self.position += 1
        return token

    def fail(self, message: str, token: Token | None = None):
        raise QuerySyntaxError(self.query, (token or self.token).position, message)

    def parse(self) -> Expression:
        if self.token.kind == Kind.END:
            self.fail("Enter search words or a quoted phrase")
        result = self.disjunction()
        if self.token.kind != Kind.END:
            self.fail(f"Unexpected {self.token.value!r}")
        return result

    def disjunction(self) -> Expression:
        children = [self.conjunction()]
        while self.token.kind == Kind.OR:
            self.take()
            children.append(self.conjunction())
        return children[0] if len(children) == 1 else AnyOf(children=tuple(children))

    def conjunction(self) -> Expression:
        children = [self.unary()]
        while self.token.kind not in {Kind.OR, Kind.RPAREN, Kind.END}:
            if self.token.kind == Kind.AND:
                self.take()
            children.append(self.unary())
        return children[0] if len(children) == 1 else AllOf(children=tuple(children))

    def unary(self) -> Expression:
        self.depth += 1
        if self.depth > 32:
            self.fail("Query nesting exceeds 32 levels")
        try:
            if self.token.kind in {Kind.MINUS, Kind.NOT}:
                self.take()
                return Negation(child=self.unary())
            if self.token.kind == Kind.LPAREN:
                self.take()
                result = self.disjunction()
                if self.token.kind != Kind.RPAREN:
                    self.fail("Expected ')' to close the group")
                self.take()
                return result
            return self.atom()
        finally:
            self.depth -= 1

    def atom(self) -> Expression:
        token = self.token
        if token.kind not in {Kind.WORD, Kind.PHRASE}:
            self.fail("Expected a word, quoted phrase or group")
        self.take()
        return Text(value=token.value, phrase=token.kind == Kind.PHRASE)


def parse_query(query: str) -> Expression:
    return Parser(query).parse()
