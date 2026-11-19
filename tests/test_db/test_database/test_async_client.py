import asyncio
import gc

from pingslurp import database


class _FakeClient:
    def __init__(self, *args, **kwargs):
        self.args = args


def test_async_client_is_cached_per_loop_object(monkeypatch):
    """A new loop must not inherit a client whose loop was destroyed.

    Caching by id(loop) can do that: CPython reuses the id after the loop is
    collected. A weak key on the loop object cannot.
    """
    monkeypatch.setattr(database, "AsyncMongoClient", _FakeClient)
    database._async_clients.clear()

    async def grab():
        return database.get_async_client()

    loop_a = asyncio.new_event_loop()
    loop_b = asyncio.new_event_loop()
    try:
        first = loop_a.run_until_complete(grab())
        again = loop_a.run_until_complete(grab())
        other = loop_b.run_until_complete(grab())
        assert first is again
        assert first is not other
        assert len(database._async_clients) == 2
    finally:
        loop_a.close()
        loop_b.close()

    del loop_a
    gc.collect()
    assert len(list(database._async_clients)) == 1
    database._async_clients.clear()
