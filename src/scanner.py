import asyncio
import enum
import inspect
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.mime import message
from functools import wraps
from timeit import default_timer as timer
from typing import Any, List, Literal, Optional

import backoff
from lighthive.client import Client
from lighthive.datastructures import Operation
from lighthive.exceptions import RPCNodeException
from lighthive.helpers.account import VOTING_MANA_REGENERATION_IN_SECONDS
from lighthive.node_picker import compare_nodes
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pydantic import BaseModel, Field, validator
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

from podping_hive.database import block_at_postion, find_big_gaps, setup_mongo_db
from podping_hive.hive_calls import HiveConnectionError, keep_checking_hive_stream
from podping_hive.podping import Podping


async def main_loop():
    # time_delta = timedelta(hours=3)
    # await keep_checking_hive_stream(time_delta=time_delta)
    setup_mongo_db()

    start_block = await block_at_postion(-1) - 50
    second_start_block = 69_253_371

    big_gaps = await find_big_gaps(time_span=timedelta(hours=2))

    if len(big_gaps) > 2:
        for i, gap in enumerate(big_gaps[2:]):
            tasks.append(
                keep_checking_hive_stream(
                    start_block=gap[0], end_block=gap[1], message=f"Gap {i}"
                )
            )

    while True:
        tasks = [
            keep_checking_hive_stream(start_block=start_block, message="Live"),
            # keep_checking_hive_stream(time_delta=timedelta(weeks=1), message="Meta"),
            # keep_checking_hive_stream(
            #     start_block=second_start_block, message="Meta", end_block=start_block
            # ),
        ]
        try:
            await asyncio.gather(*tasks)
            # await keep_checking_hive_stream(start_block=start_block)
        except HiveConnectionError:
            start_block = await block_at_postion(-1) - 50
            pass


if __name__ == "__main__":
    debug = False
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(module)-14s %(lineno) 5d : %(message)s",
        datefmt="%m-%dT%H:%M:%S",
    )
    # client = get_client()
    # logging.info(client.current_node)
    try:
        asyncio.run(main_loop())

    except asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised")
        logging.warning(ex)
        raise
    except KeyboardInterrupt:
        logging.info("Interrupted with ctrc-C")
