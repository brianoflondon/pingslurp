import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta
from random import shuffle
from timeit import default_timer as timer
from typing import List, Optional, Set, Tuple

import httpx
from beem import Hive
from beem.block import BlockHeader
from beem.blockchain import Blockchain
from beemapi.exceptions import NumRetriesReached
from httpx import URL
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import ValidationError

from pingslurp.async_wrapper import sync_to_async_iterable
from pingslurp.database import get_mongo_client, insert_podping
from pingslurp.podping import Podping


class HiveConnectionError(Exception):
    pass


# MAIN_NODES: List[str] = [
#     "https://hive-api.3speak.tv/",
#     # "https://api.pharesim.me", #Failing during HF26
#     "https://hived.emre.sh",
#     # "https://rpc.ausbit.dev",  # TypeError: string indices must be integers
#     # "https://hived.privex.io",
#     # "https://hive-api.arcange.eu",
#     # "https://rpc.ecency.com",
#     "https://api.hive.blog",  # TypeError
#     "https://api.openhive.network",
#     # "https://api.ha.deathwing.me",
#     # "https://anyx.io",
# ]

# MAIN_NODES: List[str] = ["https://rpc.podping.org/"]
MAIN_NODES: List[str] = [
    "http://hive-witness:8091/",
    "http://cepo-v4vapp:8091/",
    "https://rpc.podping.org/",
    "https://api.hive.blog/",
    "https://api.deathwing.me/",
]
# MAIN_NODES: List[str] = ["http://hive-witness:8091/"]

OP_NAMES = ["custom_json"]
HIVE_STATUS_OUTPUT_BLOCKS = 50


def seconds_only(time_delta: timedelta) -> timedelta:
    """Strip out microseconds"""
    return time_delta - timedelta(microseconds=time_delta.microseconds)


def local_api_url(endpoint: str = "") -> URL:
    """Return full local api URL"""
    LOCAL_V4VAPP_API = "http://adam-v4vapp:8000/"
    return httpx.URL(LOCAL_V4VAPP_API + endpoint)


async def send_notification_via_api(notify: str, alert_level: int) -> None:
    """Use the V4V api to send a notification"""
    try:
        async with httpx.AsyncClient() as client:
            params = {"notify": notify, "alert_level": alert_level}
            url = local_api_url("send_notification/")
            logging.debug(url)
            logging.debug(params)
            ans = await client.get(url=url, params=params, timeout=5)
            logging.debug(ans.json())
    except Exception as ex:
        logging.error(ex)
        logging.error(f"Notification failures: {ex} {ex.__class__}")


async def verify_hive_connection() -> bool:
    """Scan through all the nodes in use and see if we can get one"""
    # shuffle(MAIN_NODES)
    for node in MAIN_NODES:
        try:
            logging.info(f"Checking node: {node}")
            data = {
                "jsonrpc": "2.0",
                "method": "database_api.get_dynamic_global_properties",
                "params": {},
                "id": 1,
            }
            response = httpx.post(url=node, json=data, timeout=10.0)
            if response.status_code == 200:
                return True
            else:
                logging.warning("Connection or other Problem")
        except Exception as ex:
            logging.error(f"{ex.__class__} on node {node}")
    raise HiveConnectionError("All nodes failing")


async def get_hive_blockchain() -> Tuple[Hive, Blockchain]:
    """Wrap getting the blockchain in error catching code"""
    errors = 0
    while True:
        try:
            test = await asyncio.wait_for(verify_hive_connection(), timeout=30.0)
            if test:
                hive = Hive(node=MAIN_NODES)
                blockchain = Blockchain(blockchain_instance=hive, mode="head")
                if errors > 0:
                    message = (
                        f"Connection to Hive API working again | " f"Failures: {errors}"
                    )
                    logging.info(message)
                    asyncio.create_task(
                        send_notification_via_api(notify=message, alert_level=5),
                        name="get_hive_blockchain_error_clear_notification",
                    )
                    errors = 0
                return hive, blockchain
            else:
                raise HiveConnectionError()

        except (NumRetriesReached, asyncio.TimeoutError, HiveConnectionError) as ex:
            logging.error(f"{ex} {ex.__class__}")
            message = (
                f"Unable to connect to Hive API | "
                f"Internet connection down? | Failures: {errors}"
            )
            logging.warning(message)
            asyncio.create_task(
                send_notification_via_api(notify=message, alert_level=5),
                name="get_hive_blockchain_error_notification",
            )
            await asyncio.sleep(5 + errors * 2)
            errors += 1

        except Exception as ex:
            logging.error(f"{ex}")
            raise


def get_current_hive_block_num() -> int:
    """Returns the current Hive block number"""
    hive = Hive(node=MAIN_NODES)
    blockchain = Blockchain(blockchain_instance=hive)
    return blockchain.get_current_block_num()


def get_block_datetime(block_num: int) -> datetime:
    """Returns the datetime of a specific block in the blockchain"""
    if block_num == 0:
        block_num = 1
    block_header = BlockHeader(block=block_num)
    return block_header.time()


def get_start_block(
    blockchain: Blockchain,
    start_block: Optional[int] = None,
    time_delta: Optional[timedelta] = None,
) -> int:
    """Return the starting block"""
    if start_block:
        prev_block_num = start_block
        return prev_block_num
    elif time_delta:
        start_time = datetime.utcnow() - time_delta
        temp_blockchain = Blockchain()
        prev_block_num = temp_blockchain.get_estimated_block_num(start_time)
        return prev_block_num


def output_status(
    hive_post: dict,
    prev_block_num: int,
    counter: int,
    message: str = "",
    hive: Hive = "",
) -> Tuple[int, int, bool]:
    """Output a status line for the Hive scanner"""
    block_num = hive_post["block_num"]
    blocknum_change = False
    if block_num != prev_block_num:
        counter += 1
        blocknum_change = True
        prev_block_num = block_num
        if counter > HIVE_STATUS_OUTPUT_BLOCKS - 1:
            hive_string = f" | {hive.data.get('last_node')}" if hive else ""
            time_delta = seconds_only(
                datetime.utcnow() - hive_post["timestamp"].replace(tzinfo=None)
            )
            logging.info(
                f"{message:>8}Block: {block_num:,} | "
                f"Timedelta: {time_delta} | {hive_string}"
            )
            if time_delta < timedelta(seconds=0):
                logging.warning(
                    f"Clock might be wrong showing a time drift {time_delta}"
                )
            counter = 0
    return prev_block_num, counter, blocknum_change


async def keep_checking_hive_stream(
    start_block: Optional[int] = None,
    time_delta: Optional[timedelta] = None,
    end_block: Optional[int] = sys.maxsize,
    message: Optional[str] = "",
    database_cache: Optional[int] = 10,
) -> Tuple[int, str]:
    """
    Keeps watching the Hive stream either live or between block limits.
    Returns the last block processed and a result string when done
    """
    try:
        hive, blockchain = await get_hive_blockchain()
    except HiveConnectionError:
        logging.error("Can't connect to any Hive API Servers")
        await asyncio.sleep(1)
        raise HiveConnectionError("Can't connect to any Hive API Serverr")

    if message:
        message += " | "

    client = get_mongo_client()
    prev_block_num = get_start_block(blockchain, start_block, time_delta)
    count_new = 0
    while True:
        stream = sync_to_async_iterable(
            blockchain.stream(
                opNames=OP_NAMES,
                raw_ops=False,
                start=prev_block_num,
                max_batch_size=10,
            )
        )
        block_num = prev_block_num
        counter = 0
        if block_num:
            logging.info(
                f"{message}Starting to scan the chain at Block num: {block_num:,}"
            )
        try:
            tasks = []
            async for post in stream:
                prev_block_num, counter, block_num_change = output_status(
                    post, prev_block_num, counter, message=message, hive=hive
                )
                if len(tasks) > database_cache:
                    new_pings = await asyncio.gather(*tasks)
                    count_new += new_pings.count(True)
                    tasks = []
                if post["type"] in OP_NAMES and (
                    post.get("id").startswith("pp_")
                    or post.get("id").startswith("pplt_")
                ):
                    try:
                        podping = Podping.parse_obj(post)
                        tasks.append(
                            insert_and_report_podping(client, podping, message)
                        )
                    except ValidationError as ex:
                        logging.error("ValidationError")
                        logging.error(json.dumps(post, indent=2, default=str))
                        logging.error([post["json"]])
                        logging.error(ex)

                if post["block_num"] > end_block:
                    ret_message = (
                        f"{message:>8}Scanned from {start_block} to {end_block}. "
                        f"Finished scanning at {block_num}. New Pings: {count_new}"
                    )
                    logging.info(ret_message)
                    await asyncio.gather(*tasks)
                    return (
                        block_num,
                        ret_message,
                    )

        except asyncio.CancelledError as ex:
            await asyncio.gather(*tasks)
            logging.warning(
                "asyncio.CancelledError raised in keep_checking_hive_stream"
            )
            logging.warning(f"{ex} {ex.__class__}")
            raise
        except KeyboardInterrupt:
            await asyncio.gather(*tasks)
            raise
        except (httpx.ReadTimeout, Exception) as ex:
            await asyncio.gather(*tasks)
            asyncio.create_task(
                send_notification_via_api(
                    notify="pingslurp: Error watching Hive", alert_level=5
                ),
                name="keep_checking_hive_error_notification",
            )
            logging.error(f"Exception in Hive Watcher  {ex}")
            logging.error(ex)
            logging.warning(f"Last good block: {prev_block_num:,}")
            await asyncio.sleep(10)
            prev_block_num -= 20


async def insert_and_report_podping(
    client: AsyncIOMotorClient, podping: Podping, message: str
) -> bool:
    if await insert_podping(client, podping):
        logging.info(
            f"{message:>8}New       podping: {podping.trx_id} | {podping.required_posting_auths} | {podping.block_num}"
        )
        return True
    else:
        logging.info(
            f"{message:>8}Duplicate podping: {podping.trx_id} | {podping.required_posting_auths} | {podping.block_num}"
        )
        return False
