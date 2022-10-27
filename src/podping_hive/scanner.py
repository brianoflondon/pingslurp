import asyncio
import enum
import inspect
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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

from podping_hive.hive import get_client, listen_for_custom_json_operations
from podping_hive.podping import Podping
from podping_hive.database import get_mongo_db, insert_podping

async def main_loop():
    client = get_client()
    current_block = client.get_dynamic_global_properties()["head_block_number"]
    database = get_mongo_db("all_podpings")
    try:
        await database.create_index("trx_id", name="trx_id", unique=True)
    except Exception:
        logging.error("Can't work with this database")

    # tasks = []
    async for post in listen_for_custom_json_operations(
        condenser_api_client=client, start_block=current_block - 2000
    ):
        if post["op"][1]["id"].startswith("pp_"):
            podping = Podping.parse_obj(post)
            # tasks.append(insert_podping(database, podping))
            await insert_podping(database,podping)

            logging.info(podping)
            logging.info(datetime.utcnow() - podping.timestamp)
        # if len(tasks) > 5:
        #     start = timer()
        #     await asyncio.gather(*tasks)
        #     tasks = []
        #     logging.info(f"Database save: {timer() - start:.3f}s")


if __name__ == "__main__":
    debug = False
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(module)-14s %(lineno) 5d : %(message)s",
        datefmt="%m-%dT%H:%M:%S",
    )
    client = get_client()
    logging.info(client.current_node)
    asyncio.run(main_loop())
