import asyncio
import logging
from datetime import datetime, timedelta
from random import shuffle
from timeit import default_timer as timer
from typing import List, Optional, Set, Tuple

import httpx
from beem import Hive
from beem.blockchain import Blockchain
from beemapi.exceptions import NumRetriesReached
from httpx import URL

from podping_hive.async_wrapper import sync_to_async_iterable
from podping_hive.database import get_mongo_db, insert_podping, setup_mongo_db
from podping_hive.podping import Podping


class HiveConnectionError(Exception):
    pass


MAIN_NODES: List[str] = [
    "https://hive-api.3speak.tv/",
    # "https://api.pharesim.me", #Failing during HF26
    "https://hived.emre.sh",
    # "https://rpc.ausbit.dev",  # TypeError: string indices must be integers
    # "https://hived.privex.io",
    "https://hive-api.arcange.eu",
    # "https://rpc.ecency.com",
    "https://api.hive.blog",  # TypeError
    "https://api.openhive.network",
    # "https://api.ha.deathwing.me",
    # "https://anyx.io",
]

OP_NAMES = ["custom_json"]
HIVE_STATUS_OUTPUT_BLOCKS = 50


def seconds_only(time_delta: timedelta) -> timedelta:
    """Strip out microseconds"""
    return time_delta - timedelta(microseconds=time_delta.microseconds)


class Config:
    LOCAL_V4VAPP_API = "http://adam-v4vapp:8000/"


def local_api_url(endpoint: str = "") -> URL:
    """Return full local api URL"""
    return httpx.URL(Config.LOCAL_V4VAPP_API + endpoint)


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
    shuffle(MAIN_NODES)
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
        prev_block_num = blockchain.get_estimated_block_num(start_time)
        return prev_block_num

    prev_block_num = blockchain.get_current_block_num()

    response = httpx.get(url=local_api_url("hive/block_num"), timeout=480)
    if response.status_code == 200:
        prev_block_num = response.json()["block_num"] - 10
    else:
        time_delta = timedelta(minutes=60)
        start_time = datetime.utcnow() - time_delta
        prev_block_num = blockchain.get_estimated_block_num(start_time)
        return prev_block_num
    # start watching back this far
    return prev_block_num


def output_status(
    hive_post: dict, prev_block_num: int, counter: int
) -> Tuple[int, int, bool]:
    """Output a status line for the Hive scanner"""
    block_num = hive_post["block_num"]
    blocknum_change = False
    if block_num != prev_block_num:
        counter += 1
        blocknum_change = True
        prev_block_num = block_num
        if counter > HIVE_STATUS_OUTPUT_BLOCKS - 1:
            time_delta = seconds_only(
                datetime.utcnow() - hive_post["timestamp"].replace(tzinfo=None)
            )
            logging.info(f"Block: {block_num:,} | " f"Timedelta: {time_delta}")
            if time_delta < timedelta(seconds=0):
                logging.warning(
                    f"Clock might be wrong showing a time drift {time_delta}"
                )
            counter = 0
            response = httpx.post(
                url=local_api_url("hive/block_num"),
                params={"block_num": block_num, "time_delta": time_delta},
                timeout=480,
            )
            logging.debug(response.json())
    return prev_block_num, counter, blocknum_change


async def keep_checking_hive_stream(
    start_block: Optional[int] = None,
    time_delta: Optional[timedelta] = None,
):
    try:
        hive, blockchain = await get_hive_blockchain()
    except HiveConnectionError:
        logging.error("Can't connect to any Hive API Servers")
        await asyncio.sleep(1)
        return

    setup_mongo_db()
    database = get_mongo_db()
    try:
        await database.create_index("trx_id", name="trx_id", unique=True)
    except Exception:
        logging.error("Can't work with this database")

    prev_block_num = get_start_block(blockchain, start_block, time_delta)
    stream = sync_to_async_iterable(
        blockchain.stream(
            opNames=OP_NAMES,
            raw_ops=False,
            start=prev_block_num,
            max_batch_size=50,
        )
    )
    block_num = prev_block_num
    counter = 0
    logging.info(f"Starting to scan the chain at Block num: {block_num:,}")
    try:
        # if True:
        async for post in stream:
            prev_block_num, counter, block_num_change = output_status(
                post, prev_block_num, counter
            )
            if post["type"] in OP_NAMES and post.get("id").startswith("pp_"):
                podping = Podping.parse_obj(post)
                if await insert_podping(database, podping):
                    logging.info(
                        f"New       podping: {podping.trx_id} | {podping.required_posting_auths}"
                    )
                else:
                    logging.info(
                        f"Duplicate podping: {podping.trx_id} | {podping.required_posting_auths}"
                    )

    except asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised in keep_checking_hive_stream")
        logging.warning(f"{ex} {ex.__class__}")
        raise
    except KeyboardInterrupt:
        raise
    except (httpx.ReadTimeout, Exception) as ex:
        asyncio.create_task(
            send_notification_via_api(
                notify="hive_scanner: Error watching Hive", alert_level=5
            ),
            name="keep_checking_hive_error_notification",
        )
        logging.error(f"Exception in Hive Watcher  {ex}")
        logging.error(ex)
        logging.warning(f"Last good block: {prev_block_num:,}")
        await asyncio.sleep(10)
        asyncio.create_task(keep_checking_hive_stream(start_block=prev_block_num - 50))
