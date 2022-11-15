import logging
import os
import sys

from dotenv import load_dotenv

debug = False
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO if not debug else logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(module)-14s %(lineno) 5d : %(message)s",
    datefmt="%m-%dT%H:%M:%S",
)


class Config:

    load_dotenv()

    DB_CONNECTION = os.getenv("DB_CONNECTION")
    ROOT_DB_NAME = os.getenv("ROOT_DB_NAME")
    COLLECTION_NAME = "all_podpings"
    COLLECTION_NAME_META = "meta_ts"
    COLLECTION_NAME_HOSTS = "hosts_ts"

class StateOptions():
    verbose: bool = False
