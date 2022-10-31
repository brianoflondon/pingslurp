import logging

import pytest

from podping_hive.database import (
    all_blocks,
    all_blocks_it,
    block_at_postion,
    find_big_gaps,
    range_extract,
)
from podping_hive.hive_calls import get_block_datetime, get_hive_blockchain


@pytest.mark.asyncio
async def test_all_blocks():
    ans = await all_blocks()
    assert ans
    ans_it = []
    async for block_num in all_blocks_it():
        ans_it.append(block_num)
    assert ans == ans_it


@pytest.mark.asyncio
async def test_first_last_blocks():
    fb = await block_at_postion(0)
    assert fb
    lb = await block_at_postion(position=-1)
    mid = await block_at_postion(position=5)
    assert fb < mid < lb


@pytest.mark.asyncio
async def test_range_extract():
    last_block = 0
    async for range_block in range_extract(all_blocks_it()):
        if range_block[0] - last_block > 50:
            logging.info(f"Big gap at: {range_block}")
        # if len(range_block) > 1:
        # logging.info(range_block)
        last_block = max(range_block)
    assert True


@pytest.mark.asyncio
async def test_find_big_gaps():
    ans = await find_big_gaps(500)
    date_gaps = []
    for start, end in ans:
        start = get_block_datetime(start)
        end = get_block_datetime(end)
        date_gaps.append(
            (start, end)
        )
        logging.info(f"Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")
    logging.info(ans)
    logging.info(date_gaps)
    assert ans
