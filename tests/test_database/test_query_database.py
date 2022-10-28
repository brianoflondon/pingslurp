import pytest

from podping_hive.database import all_blocks, all_blocks_it, block_at_postion


@pytest.mark.asyncio
async def test_all_blocks():
    ans = await all_blocks()
    assert ans
    ans_it = set()
    async for block_num in all_blocks_it():
        ans_it.add(block_num)
    assert ans == ans_it

@pytest.mark.asyncio
async def test_first_last_blocks():
    fb = await block_at_postion(0)
    assert fb
    lb = await block_at_postion(position = -1)
    mid = await block_at_postion(position = 5)
    assert fb < mid < lb
