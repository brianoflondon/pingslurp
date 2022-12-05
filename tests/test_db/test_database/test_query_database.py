import json
import logging
from datetime import timedelta

import pytest

from pingslurp.config import Config
from pingslurp.database import (
    all_blocks,
    all_blocks_it,
    block_at_postion,
    find_big_gaps,
    get_mongo_db,
    range_extract,
)
from pingslurp.hive_calls import get_block_datetime, get_hive_blockchain
from pingslurp.podping_schemas import Podping


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
async def test_meta_first_last_blocks():
    db = get_mongo_db(Config.COLLECTION_NAME_META)
    fb = await block_at_postion(0, collection=db)
    assert fb
    lb = await block_at_postion(position=-1, collection=db)
    mid = await block_at_postion(position=5, collection=db)
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
    ans = await find_big_gaps(time_span=timedelta(hours=2))
    date_gaps = []
    for start, end in ans:
        start = get_block_datetime(start)
        end = get_block_datetime(end)
        date_gaps.append((start, end))
        logging.info(f"Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")
    logging.info(ans)
    logging.info(date_gaps)
    assert ans


@pytest.mark.asyncio
async def test_meta_find_big_gaps():
    db = get_mongo_db(Config.COLLECTION_NAME_META)
    ans = await find_big_gaps(time_span=timedelta(hours=1), db=db)
    date_gaps = []
    for start, end in ans:
        start = get_block_datetime(start)
        end = get_block_datetime(end)
        date_gaps.append((start, end))
        logging.info(f"Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")
    logging.info(ans)
    logging.info(date_gaps)
    assert ans


@pytest.mark.asyncio
async def test_check_hosts():
    db = get_mongo_db(Config.COLLECTION_NAME)
    cursor = db.find({}, {"_id": 0}).limit(20)
    async for doc in cursor:
        pp = Podping.parse_obj(doc)
        for iri in pp.iris:
            # logging.info(iri.__repr__())
            logging.info(f"{iri.host:<20} | {iri:>30}")
        # logging.info(pp)


@pytest.mark.asyncio
async def test_check_podping_formats():
    db = get_mongo_db(Config.COLLECTION_NAME)
    cursor = db.find({}, {"_id": 0}).limit(20)
    async for doc in cursor:
        pp = Podping.parse_obj(doc)

        # logging.info(pp.db_format_hosts_ts())
        logging.info(json.dumps(pp.db_format_hosts_ts(), indent=2, default=str))
        # logging.info(pp)
