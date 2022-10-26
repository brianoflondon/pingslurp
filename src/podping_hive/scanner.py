import asyncio
import inspect
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from timeit import default_timer as timer
from typing import List, Optional

import backoff
from lighthive.client import Client
from lighthive.datastructures import Operation
from lighthive.exceptions import RPCNodeException
from lighthive.helpers.account import VOTING_MANA_REGENERATION_IN_SECONDS
from lighthive.node_picker import compare_nodes
from pydantic import BaseModel, Field

from podping_hive.hive import get_client, listen_for_custom_json_operations


class HiveTrx(BaseModel):
    trx_id: str = Field(alias="id")
    block_num: int
    trx_num: int
    expired: bool


async def main_loop():
    client = get_client()
    current_block = client.get_dynamic_global_properties()["head_block_number"]
    async for post in listen_for_custom_json_operations(
        condenser_api_client=client, start_block=current_block - 200
    ):
        if post["op"][1]["id"].startswith("pp_"):
            logging.info(post)


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
