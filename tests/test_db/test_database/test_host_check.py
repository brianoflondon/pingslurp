from pingslurp.host_check import check_hosts


import pytest


@pytest.mark.asyncio
async def test_check_hosts():
    nodes_tested = await check_hosts()
    print(nodes_tested)
    assert True
