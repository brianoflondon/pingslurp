from asyncio import exceptions

import pytest

from pingslurp.hive_calls import (
    get_hive_blockchain,
    send_notification_via_api,
    verify_hive_connection,
)


@pytest.mark.asyncio
async def test_send_notification_via_api():
    await send_notification_via_api("Just testing", alert_level=1)


@pytest.mark.asyncio
async def test_verify_hive_connection():
    try:
        working = await verify_hive_connection()
        assert working
    except Exception as e:
        print(e)
        assert False


@pytest.mark.asyncio
async def test_get_hive_blockchain():
    try:
        hive = await get_hive_blockchain()
    except Exception as e:
        assert False
