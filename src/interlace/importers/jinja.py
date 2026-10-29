"""The slice of Jinja a dbt model actually uses.

Not a Jinja implementation. dbt models in the projects people migrate (the jaffle
shops, the MRR playbook) need ``ref``, ``source``, ``config``, ``var``, a
``{% set %}`` of a list, a ``{% for %}`` over it, ``is_incremental()``, and
macro calls whose body is a SQL fragment. Everything else is reported and left
unconverted, rather than half-rendered into SQL that looks like it compiled.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class UnsupportedJinja(Exception):
    """A construct this importer will not guess at."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class Macro:
    """A ``{% macro %}`` body. Arguments are names; the body is still Jinja."""

    name: str
    params: tuple[str, ...]
    body: str


@dataclass(frozen=True)
class _Name:
    """A bare word, as opposed to a string literal with the same spelling."""

    text: str


@dataclass(frozen=True)
class _Call:
    func: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class _Dispatch:
    """``adapter.dispatch('name')`` — resolved to ``default__name`` when called."""

    macro: str


@dataclass
class _Piece:
    kind: str  # text, expr, tag
    text: str
    strip_left: bool = False
    strip_right: bool = False


@dataclass
class Rendered:
    """A template that converted, plus the ``config()`` kwargs it declared."""

    sql: str
    config: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


_GRAINS = frozenset({"day", "week", "month", "quarter", "year", "hour"})
_MISSING = object()


def parse_macros(text: str) -> dict[str, Macro]:
    """Every ``{% macro %}`` in a file, keyed by name. Nested macros are rejected."""
    pieces = _tokenize(text)
    macros: dict[str, Macro] = {}
    index = 0
    while index < len(pieces):
        piece = pieces[index]
        if piece.kind != "tag" or not piece.text.startswith("macro "):
            index += 1
            continue
        header = piece.text[len("macro ") :]
        name, params = _macro_signature(header)
        index += 1
        body: list[_Piece] = []
        depth = 1
        while index < len(pieces):
            inner = pieces[index]
            if inner.kind == "tag" and inner.text.startswith("macro "):
                depth += 1
            elif inner.kind == "tag" and inner.text == "endmacro":
                depth -= 1
                if depth == 0:
                    break
            body.append(inner)
            index += 1
        else:
            raise UnsupportedJinja(f"macro {name} is missing endmacro")
        macros[name] = Macro(name, params, _detokenize(body))
        index += 1
    return macros


def render_template(
    text: str,
    *,
    macros: dict[str, Macro] | None = None,
    variables: dict[str, Any] | None = None,
    model_name: str = "",
) -> Rendered:
    """Render one model. Raises :class:`UnsupportedJinja` if a construct is out of scope."""
    rendered = Rendered(sql="")
    env = _Env(
        scope=dict(variables or {}),
        macros=dict(macros or {}),
        rendered=rendered,
        model_name=model_name,
    )
    rendered.sql = env.render(_tokenize(text), 0, None).strip() + "\n"
    rendered.config = env.inline
    return rendered


@dataclass
class _Env:
    scope: dict[str, Any]
    macros: dict[str, Macro]
    rendered: Rendered
    model_name: str
    inline: dict[str, Any] = field(default_factory=dict)
    _warned_incremental: bool = False

    def render(self, pieces: list[_Piece], start: int, stop: int | None) -> str:
        out = _Out()
        index = start
        end = len(pieces) if stop is None else stop
        while index < end:
            piece = pieces[index]
            if piece.strip_left:
                out.strip_left()
            if piece.kind == "text":
                out.text(piece.text)
            elif piece.kind == "expr":
                out.text(_paste(self._realize(_parse_value(piece.text))))
            elif piece.kind == "tag":
                index = self._tag(pieces, index, end, out)
                continue
            if piece.strip_right:
                out.strip_right()
            index += 1
        return out.value()

    def _tag(self, pieces: list[_Piece], index: int, end: int, out: _Out) -> int:
        piece = pieces[index]
        words = piece.text.split(None, 1)
        keyword = words[0] if words else ""
        rest = words[1] if len(words) > 1 else ""
        if keyword == "set":
            self._set(rest)
            return self._close(piece, index)
        if keyword == "for":
            return self._for(pieces, index, end, out, rest, piece.strip_right)
        if keyword == "if":
            return self._if(pieces, index, end, out, rest)
        if keyword in {"endfor", "endif", "elif", "else", "endmacro", "macro"}:
            raise UnsupportedJinja(f"unexpected {{% {keyword} %}}")
        raise UnsupportedJinja(f"{{% {keyword} %}} is not converted")

    def _close(self, piece: _Piece, index: int) -> int:
        return index + 1

    def _set(self, rest: str) -> None:
        if "=" not in rest:
            raise UnsupportedJinja(f"{{% set {rest} %}} is not a simple assignment")
        name, expr = rest.split("=", 1)
        name = name.strip()
        if not name.isidentifier():
            raise UnsupportedJinja(f"{{% set {name} %}} is not a simple assignment")
        self.scope[name] = self._realize(_parse_value(expr.strip()))

    def _for(self, pieces: list[_Piece], index: int, end: int, out: _Out, rest: str, strip_right: bool) -> int:
        parts = rest.split(" in ", 1)
        if len(parts) != 2 or not parts[0].strip().isidentifier():
            raise UnsupportedJinja(f"{{% for {rest} %}} is not converted")
        var = parts[0].strip()
        values = self._realize(_parse_value(parts[1].strip()))
        if not isinstance(values, list):
            raise UnsupportedJinja(f"{{% for {var} in {parts[1].strip()} %}} did not resolve to a list")
        close = _match(pieces, index + 1, end, "for", "endfor")
        body = pieces[index + 1 : close]
        saved = self.scope.get(var, _MISSING)
        for item in values:
            self.scope[var] = item
            out.text(self.render(body, 0, None))
        if saved is _MISSING:
            self.scope.pop(var, None)
        else:
            self.scope[var] = saved
        if strip_right or (close < len(pieces) and pieces[close].strip_right):
            out.strip_right()
        return close + 1

    def _if(self, pieces: list[_Piece], index: int, end: int, out: _Out, condition: str) -> int:
        branches: list[tuple[str | None, int, int]] = []
        cursor = index
        cond: str | None = condition
        endif_at = index
        while cursor < end:
            close = _match_if(pieces, cursor + 1, end)
            branches.append((cond, cursor + 1, close))
            tag = pieces[close].text
            if tag == "endif":
                endif_at = close
                break
            if tag == "else":
                cond = None
            elif tag.startswith("elif "):
                cond = tag[len("elif ") :]
            else:
                raise UnsupportedJinja(f"unexpected {{% {tag} %}}")
            cursor = close
        else:
            raise UnsupportedJinja("{% if %} is missing endif")
        taken = False
        for branch_cond, start, stop in branches:
            if taken:
                break
            if branch_cond is None or self._truthy(branch_cond):
                out.text(self.render(pieces[start:stop], 0, None))
                taken = True
        return endif_at + 1

    def _truthy(self, expr: str) -> bool:
        return bool(self._realize(_parse_value(expr.strip())))

    def _call(self, call: _Call) -> Any:
        args = tuple(self._realize(arg) for arg in call.args)
        kwargs = {key: self._realize(value) for key, value in call.kwargs.items()}
        func = call.func
        if isinstance(func, _Name):
            func = func.text
        elif isinstance(func, _Call):
            func = self._call(func)
        if isinstance(func, _Dispatch):
            return self._macro(f"default__{func.macro}", args, kwargs)
        if not isinstance(func, str):
            raise UnsupportedJinja("that call shape is not converted")
        return self._named(func, args, kwargs)

    def _realize(self, value: Any) -> Any:
        if isinstance(value, _Call):
            return self._call(value)
        if isinstance(value, _Name):
            if value.text == "this":
                return self.model_name
            if value.text in self.scope:
                return self.scope[value.text]
            raise UnsupportedJinja(f"{{{{{value.text}}}}} is not defined")
        if isinstance(value, list):
            return [self._realize(item) for item in value]
        return value

    def _named(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:  # noqa: C901
        if name in self.scope and not args and not kwargs:
            return self.scope[name]
        if name in self.macros:
            return self._macro(name, args, kwargs)
        if name == "ref":
            return _ident(args[-1] if args else "", "ref")
        if name == "source":
            if len(args) < 2:
                raise UnsupportedJinja("source() needs a source name and a table name")
            return _ident(args[1], "source")
        if name == "var":
            return self._var(args)
        if name == "this":
            return self.model_name
        if name == "config":
            self.inline.update(kwargs)
            for arg in args:
                if isinstance(arg, dict):
                    self.inline.update(arg)
            return ""
        if name == "return":
            return args[0] if args else ""
        if name == "is_incremental":
            if not self._warned_incremental:
                self.rendered.warnings.append(
                    "is_incremental() was taken as false: interlace windows an incremental "
                    "model with interval and time_column, so the full-refresh SQL was kept"
                )
                self._warned_incremental = True
            return False
        if name == "adapter.dispatch":
            if not args:
                raise UnsupportedJinja("adapter.dispatch() needs a macro name")
            return _Dispatch(str(args[0]))
        if name in {"dbt.date_trunc", "date_trunc"}:
            return _date_trunc(args)
        if name in {"dbt.date_spine", "dbt_utils.date_spine"}:
            return _date_spine(args, kwargs)
        if name in {"dbt_utils.generate_surrogate_key", "dbt_utils.surrogate_key"}:
            return _surrogate(args, kwargs)
        if name in {"dbt.current_timestamp", "dbt.current_timestamp_in_utc"}:
            return "current_timestamp"
        raise UnsupportedJinja(f"{{{{{name}()}}}} is not converted")

    def _var(self, args: tuple[Any, ...]) -> Any:
        if not args:
            raise UnsupportedJinja("var() needs a name")
        key = str(args[0])
        if key in self.scope:
            return self.scope[key]
        if len(args) > 1:
            return args[1]
        raise UnsupportedJinja(f"var('{key}') is not defined on the dbt project")

    def _macro(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        macro = self.macros.get(name)
        if macro is None:
            raise UnsupportedJinja(f"macro {name} is not defined in the project")
        bound = dict(self.scope)
        for index, param in enumerate(macro.params):
            if index < len(args):
                bound[param] = args[index]
        bound.update(kwargs)
        child = _Env(scope=bound, macros=self.macros, rendered=self.rendered, model_name=self.model_name)
        child.inline = self.inline
        return child.render(_tokenize(macro.body), 0, None)


class _Out:
    def __init__(self) -> None:
        self._parts: list[str] = []
        self._lstrip = False

    def text(self, value: str) -> None:
        if self._lstrip:
            value = value.lstrip()
            self._lstrip = False
        if value:
            self._parts.append(value)

    def strip_left(self) -> None:
        while self._parts and not self._parts[-1].rstrip():
            self._parts.pop()
        if self._parts:
            self._parts[-1] = self._parts[-1].rstrip()

    def strip_right(self) -> None:
        self._lstrip = True

    def value(self) -> str:
        return "".join(self._parts)


def _tokenize(text: str) -> list[_Piece]:
    pieces: list[_Piece] = []
    index = 0
    length = len(text)
    while index < length:
        start = _find_open(text, index)
        if start is None:
            pieces.append(_Piece("text", text[index:]))
            break
        if start > index:
            pieces.append(_Piece("text", text[index:start]))
        kind, cursor, strip_left = _open(text, start)
        closer = {"comment": "#}", "expr": "}}", "tag": "%}"}[kind]
        end = text.find(closer, cursor)
        if end == -1:
            raise UnsupportedJinja("an unclosed Jinja tag")
        strip_right = end > cursor and text[end - 1] == "-"
        body = text[cursor : end - 1 if strip_right else end]
        if kind != "comment":
            pieces.append(_Piece(kind, body.strip(), strip_left, strip_right))
        elif strip_left or strip_right:
            pieces.append(_Piece("text", "", strip_left, strip_right))
        index = end + len(closer)
    return pieces


def _find_open(text: str, index: int) -> int | None:
    while index < len(text):
        found = text.find("{", index)
        if found == -1 or found + 1 >= len(text):
            return None
        if text[found + 1] in {"#", "{", "%"}:
            return found
        index = found + 1
    return None


def _open(text: str, index: int) -> tuple[str, int, bool]:
    kind = {"#": "comment", "{": "expr", "%": "tag"}[text[index + 1]]
    cursor = index + 2
    strip_left = cursor < len(text) and text[cursor] == "-"
    return kind, cursor + (1 if strip_left else 0), strip_left


def _detokenize(pieces: list[_Piece]) -> str:
    """Rebuild a macro body so it can be tokenized again when the macro is called."""
    chunks: list[str] = []
    for piece in pieces:
        if piece.kind == "text":
            chunks.append(piece.text)
            continue
        left = "-" if piece.strip_left else ""
        right = "-" if piece.strip_right else ""
        if piece.kind == "expr":
            chunks.append("{{" + left + piece.text + right + "}}")
        else:
            chunks.append("{%" + left + " " + piece.text + " " + right + "%}")
    return "".join(chunks)


def _match(pieces: list[_Piece], start: int, end: int, open_kw: str, close_kw: str) -> int:
    depth = 1
    for index in range(start, end):
        piece = pieces[index]
        if piece.kind != "tag":
            continue
        keyword = piece.text.split(None, 1)[0]
        if keyword == open_kw:
            depth += 1
        elif keyword == close_kw:
            depth -= 1
            if depth == 0:
                return index
    raise UnsupportedJinja(f"{{% {open_kw} %}} is missing {close_kw}")


def _match_if(pieces: list[_Piece], start: int, end: int) -> int:
    """The next elif/else/endif at this if's depth."""
    depth = 0
    for index in range(start, end):
        piece = pieces[index]
        if piece.kind != "tag":
            continue
        keyword = piece.text.split(None, 1)[0]
        if keyword == "if":
            depth += 1
        elif keyword == "endif" and depth:
            depth -= 1
        elif depth == 0 and keyword in {"elif", "else", "endif"}:
            return index
    raise UnsupportedJinja("{% if %} is missing endif")


def _macro_signature(header: str) -> tuple[str, tuple[str, ...]]:
    open_paren = header.find("(")
    if open_paren == -1 or not header.endswith(")"):
        raise UnsupportedJinja(f"macro signature {header!r} is not converted")
    name = header[:open_paren].strip()
    if not name.isidentifier():
        raise UnsupportedJinja(f"macro name {name!r} is not an identifier")
    raw = header[open_paren + 1 : -1].strip()
    if not raw:
        return name, ()
    params = tuple(part.strip() for part in raw.split(","))
    if not all(param.isidentifier() for param in params):
        raise UnsupportedJinja(f"macro {name} has a parameter default or unpacking, which is not converted")
    return name, params


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.index = 0

    def skip(self) -> None:
        while self.index < len(self.text) and self.text[self.index] in " \t\r\n":
            self.index += 1

    def parse(self) -> Any:
        value = self._atom()
        self.skip()
        if self.index < len(self.text):
            raise UnsupportedJinja(f"could not read {self.text[self.index :]!r}")
        return value

    def _atom(self) -> Any:
        self.skip()
        if self.index >= len(self.text):
            raise UnsupportedJinja("a Jinja expression was empty")
        char = self.text[self.index]
        if char in {"'", '"'}:
            return self._string()
        if char == "[":
            return self._list()
        if char == "-" or char.isdigit():
            return self._number()
        word = self._word()
        if word == "true":
            return True
        if word == "false":
            return False
        if word in {"none", "null"}:
            return None
        node: Any = _Name(word)
        while True:
            self.skip()
            if self.index < len(self.text) and self.text[self.index] == "(":
                args, kwargs = self._args()
                node = _Call(node, args, kwargs)
                continue
            return node

    def _word(self) -> str:
        start = self.index
        while self.index < len(self.text) and (self.text[self.index].isalnum() or self.text[self.index] in {"_", "."}):
            self.index += 1
        if self.index == start:
            raise UnsupportedJinja(f"could not read {self.text[self.index :]!r}")
        return self.text[start : self.index]

    def _string(self) -> str:
        quote = self.text[self.index]
        self.index += 1
        chars: list[str] = []
        while self.index < len(self.text):
            char = self.text[self.index]
            if char == "\\":
                self.index += 1
                if self.index >= len(self.text):
                    break
                chars.append(self.text[self.index])
            elif char == quote:
                self.index += 1
                return "".join(chars)
            else:
                chars.append(char)
            self.index += 1
        raise UnsupportedJinja("an unclosed string")

    def _number(self) -> int | float | str:
        start = self.index
        if self.text[self.index] == "-":
            self.index += 1
        saw_digit = False
        while self.index < len(self.text) and self.text[self.index].isdigit():
            saw_digit = True
            self.index += 1
        if self.index < len(self.text) and self.text[self.index] == ".":
            self.index += 1
            while self.index < len(self.text) and self.text[self.index].isdigit():
                saw_digit = True
                self.index += 1
            if saw_digit:
                return float(self.text[start : self.index])
        if saw_digit and "." not in self.text[start : self.index]:
            return int(self.text[start : self.index])
        raise UnsupportedJinja(f"could not read {self.text[start:]!r}")

    def _list(self) -> list[Any]:
        self.index += 1
        values: list[Any] = []
        while True:
            self.skip()
            if self.index < len(self.text) and self.text[self.index] == "]":
                self.index += 1
                return values
            values.append(self._atom())
            self.skip()
            if self.index < len(self.text) and self.text[self.index] == ",":
                self.index += 1
                continue
            if self.index < len(self.text) and self.text[self.index] == "]":
                self.index += 1
                return values
            raise UnsupportedJinja("a list was not closed")

    def _args(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        self.index += 1  # (
        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        while True:
            self.skip()
            if self.index < len(self.text) and self.text[self.index] == ")":
                self.index += 1
                return tuple(args), kwargs
            if self._looks_kwarg():
                name = self._word()
                self.skip()
                self.index += 1  # =
                kwargs[name] = self._atom()
            else:
                args.append(self._atom())
            self.skip()
            if self.index < len(self.text) and self.text[self.index] == ",":
                self.index += 1

    def _looks_kwarg(self) -> bool:
        cursor = self.index
        if cursor >= len(self.text) or not (self.text[cursor].isalpha() or self.text[cursor] == "_"):
            return False
        while cursor < len(self.text) and (self.text[cursor].isalnum() or self.text[cursor] == "_"):
            cursor += 1
        while cursor < len(self.text) and self.text[cursor] in " \t\r\n":
            cursor += 1
        return cursor < len(self.text) and self.text[cursor] == "="


def _parse_value(text: str) -> Any:
    if not text:
        raise UnsupportedJinja("a Jinja expression was empty")
    return _Parser(text).parse()


def _paste(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _ident(value: Any, what: str) -> str:
    text = str(value)
    if not text.isidentifier():
        raise UnsupportedJinja(f"{what}({text!r}) is not a plain identifier")
    return text


def _date_trunc(args: tuple[Any, ...]) -> str:
    if len(args) != 2:
        raise UnsupportedJinja("dbt.date_trunc needs a part and a column")
    part = str(args[0])
    column = str(args[1])
    if part not in _GRAINS or not column.isidentifier():
        raise UnsupportedJinja(f"dbt.date_trunc({part!r}, {column!r}) is not converted")
    return f"date_trunc('{part}', {column})"


def _date_spine(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    grain = str(kwargs.get("datepart", args[0] if args else ""))
    start = kwargs.get("start_date", args[1] if len(args) > 1 else None)
    end = kwargs.get("end_date", args[2] if len(args) > 2 else None)
    if grain not in _GRAINS or start is None or end is None:
        raise UnsupportedJinja("date_spine needs a grain, a start date and an end date")
    # dbt's spine is exclusive of the end. The column name is date_<grain>.
    return (
        f"select generate_series as date_{grain} from generate_series"
        f"({start}, ({end}) - interval 1 {grain}, interval 1 {grain})"
    )


def _surrogate(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    columns = kwargs.get("field_list", args[0] if args else None)
    if isinstance(columns, str):
        columns = list(args) if len(args) > 1 else [columns]
    if not isinstance(columns, list) or not columns or not all(str(column).isidentifier() for column in columns):
        raise UnsupportedJinja("surrogate_key needs a list of column names")
    parts = ", ".join(f"coalesce(cast({column} as varchar), '_dbt_utils_surrogate_key_null_')" for column in columns)
    return f"md5(concat_ws('-', {parts}))"
