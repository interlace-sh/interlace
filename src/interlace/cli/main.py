"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

# Importing the command modules registers them on ``app``.
import interlace.cli.build  # noqa: F401
import interlace.cli.inspect  # noqa: F401
import interlace.cli.operate  # noqa: F401
from interlace.cli.support import _flatten_exceptions, _print_error, app
from interlace.exceptions import InterlaceError


def main() -> None:
    try:
        app()
    except InterlaceError as exc:  # expected, user-facing errors: one clean line, no traceback
        _print_error(exc)
        raise SystemExit(1) from None
    except BaseExceptionGroup as group:
        # A parallel apply surfaces failures as an ExceptionGroup. When every leaf is a
        # user-facing InterlaceError (e.g. several models failed their definition/checks),
        # print one clean line each instead of dumping the group traceback; a genuine
        # internal error in the mix still propagates with its trace.
        leaves = _flatten_exceptions(group)
        if leaves and all(isinstance(leaf, InterlaceError) for leaf in leaves):
            for leaf in dict.fromkeys(leaf for leaf in leaves if isinstance(leaf, InterlaceError)):
                _print_error(leaf)
            raise SystemExit(1) from None
        raise
