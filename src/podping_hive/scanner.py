import logging
import os
from typing import List, Optional

import backoff
from lighthive.client import Client
from lighthive.datastructures import Operation
from lighthive.exceptions import RPCNodeException
from lighthive.helpers.account import VOTING_MANA_REGENERATION_IN_SECONDS
from lighthive.node_picker import compare_nodes
from pydantic import BaseModel, Field



class HiveTrx(BaseModel):
    trx_id: str = Field(alias="id")
    block_num: int
    trx_num: int
    expired: bool


def get_client(
    posting_keys: Optional[List[str]] = None,
    nodes=None,
    connect_timeout=3,
    read_timeout=30,
    loglevel=logging.ERROR,
    chain=None,
    automatic_node_selection=False,
    api_type="condenser_api",
) -> Client:
    try:
        if os.getenv("TESTNET", "False").lower() in (
            "true",
            "1",
            "t",
        ):
            nodes = [os.getenv("TESTNET_NODE")]
            chain = {"chain_id": os.getenv("TESTNET_CHAINID")}
        else:
            nodes = [
                "https://hived.emre.sh",
                "https://api.hive.blog",
                "https://api.deathwing.me",
                # "https://hive-api.arcange.eu",
                "https://api.openhive.network",
                " https://rpc.ausbit.dev",
            ]
        client = Client(
            keys=posting_keys,
            nodes=nodes,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            loglevel=loglevel,
            chain=chain,
            automatic_node_selection=automatic_node_selection,
            backoff_mode=backoff.fibo,
            backoff_max_tries=3,
            load_balance_nodes=True,
            circuit_breaker=True,
        )
        return client(api_type)
    except Exception as ex:
        logging.error("Error getting Hive Client")
        logging.exception(ex)
        raise ex


if __name__ == "__main__":
    debug = False
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(module)-14s %(lineno) 5d : %(message)s",
        datefmt="%m-%dT%H:%M:%S",
    )
    client = get_client()
    logging.info(client.current_node)
