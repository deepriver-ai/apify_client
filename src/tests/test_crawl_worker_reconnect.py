"""consume_forever: the crawl worker survives a lost runs connection (2026-10-07)."""
from __future__ import annotations

import pytest
from pika.exceptions import AMQPConnectionError, AMQPHeartbeatTimeout, StreamLostError

from src.run_crawl_worker import consume_forever


def _consumer(outcomes):
    calls = []

    def consume_fn(deps, queue, once=False, concurrency=1):
        calls.append({"queue": queue, "once": once, "concurrency": concurrency})
        exc = outcomes.pop(0)
        if exc is not None:
            raise exc

    return consume_fn, calls


def test_reconnects_with_backoff_until_a_clean_stop():
    consume_fn, calls = _consumer([AMQPHeartbeatTimeout("no heartbeat"), StreamLostError("EOF"),
                                   AMQPConnectionError("handshake timeout"), None])
    sleeps = []
    consume_forever(None, "dev_crawl_runs", concurrency=4, consume_fn=consume_fn, sleep=sleeps.append, max_backoff=5)
    assert len(calls) == 4 and all(c == {"queue": "dev_crawl_runs", "once": False, "concurrency": 4} for c in calls)
    assert sleeps == [2.0, 4.0, 5]  # doubling, capped


def test_other_errors_still_end_the_process():
    consume_fn, calls = _consumer([ValueError("bug")])
    with pytest.raises(ValueError):
        consume_forever(None, "q", consume_fn=consume_fn, sleep=lambda s: None)
    assert len(calls) == 1


def test_max_reconnects_bounds_the_loop():
    consume_fn, calls = _consumer([StreamLostError("EOF")] * 3)
    with pytest.raises(StreamLostError):
        consume_forever(None, "q", consume_fn=consume_fn, sleep=lambda s: None, max_reconnects=2)
    assert len(calls) == 3
