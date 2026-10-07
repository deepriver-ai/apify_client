"""Atomic JSON writes for the on-disk caches shared by concurrent crawl runs.

The crawl worker runs several runs in one process (``CRAWL_WORKER_CONCURRENCY``), so a
cache file can be written by two threads at once. Writing to a temp file in the same
directory and ``os.replace``-ing it means a reader never sees a torn file (a torn
``filter_cache.json`` used to load as "starting fresh" and was then saved back empty).
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any


def atomic_write_json(path: str, obj: Any, **dump_kwargs: Any) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, **dump_kwargs)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
