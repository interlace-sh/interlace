"""A RecordBatchReader that releases whatever it borrowed.

PyArrow does not run a ``from_batches`` generator's ``finally`` when the reader
is closed or dropped, only when it is read to the end. Callers that abandon a
reader would otherwise keep the cursor, the result buffer, or a spill file for
the life of the process.

``own_reader`` runs ``release`` on the thread that exhausts the reader, and on
``release_owned``. Dropping the reader without reading runs ``defer`` from a
finalizer. Pass a ``defer`` that does not touch a live DuckDB connection: the
finalizer runs on the garbage-collection thread, and closing a cursor there
while another cursor is mid-query aborts the process. The adapter closes those
deferred cursors on its next use of the connection, and on ``close()``.
"""

from __future__ import annotations

import contextlib
import threading
import weakref
from collections.abc import Callable, Iterator

import pyarrow as pa

_owned: dict[int, Callable[[], None]] = {}


def own_reader(
    reader: pa.RecordBatchReader,
    *,
    release: Callable[[], None],
    defer: Callable[[], None] | None = None,
) -> pa.RecordBatchReader:
    """Return ``reader`` with ``release`` bound to its lifetime.

    ``defer`` defaults to ``release``. DuckDB passes a deferral that only
    records the cursor; ``release`` is what actually closes it.
    """
    state = {"done": False}
    gate = threading.Lock()
    held = {"id": 0}

    def finish(immediate: bool) -> None:
        with gate:
            if state["done"]:
                return
            state["done"] = True
        _owned.pop(held["id"], None)
        action = release if immediate or defer is None else defer
        with contextlib.suppress(Exception):
            action()

    def batches() -> Iterator[pa.RecordBatch]:
        # Exhaustion and a read error run on the caller's thread, so release
        # now. GeneratorExit is the reader being dropped mid-stream, often from
        # the garbage collector: defer instead of releasing here.
        try:
            yield from reader
        except BaseException as exc:
            finish(not isinstance(exc, GeneratorExit))
            raise
        else:
            finish(True)

    wrapped = pa.RecordBatchReader.from_batches(reader.schema, batches())
    held["id"] = id(wrapped)
    _owned[held["id"]] = lambda: finish(True)
    weakref.finalize(wrapped, finish, False)
    return wrapped


def release_owned(reader: pa.RecordBatchReader | None) -> None:
    """Release a reader this process owns, on the caller's thread.

    A reader that was not registered is just closed.
    """
    if reader is None:
        return
    closer = _owned.pop(id(reader), None)
    if closer is not None:
        closer()
    with contextlib.suppress(Exception):
        reader.close()
