"""Lease renewal off the event loop.

A model may run for hours. The renewal that proves the worker is still alive
cannot live on the event loop: an ``async`` model that blocks, or any other
call that does not yield, would stop the heartbeat and let another process
reclaim a run that is still going. A thread renews the row directly.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

logger = logging.getLogger("interlace.lease")


def start_heartbeat(name: str, interval: float, renew: Callable[[], bool]) -> tuple[threading.Event, threading.Thread]:
    """Renew until ``renew`` returns False or :func:`stop_heartbeat` is called.

    ``renew`` runs on the heartbeat thread. A raised exception is logged and
    the next interval tries again — one locked database must not abandon the run.
    """
    stop = threading.Event()

    def run() -> None:
        while not stop.wait(interval):
            try:
                alive = renew()
            except Exception:
                logger.exception("%s heartbeat failed; will retry", name)
                continue
            if not alive:
                return

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.start()
    return stop, thread


def stop_heartbeat(stop: threading.Event, thread: threading.Thread) -> None:
    stop.set()
    thread.join(timeout=2)
