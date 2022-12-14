import logging
from datetime import timedelta
from typing import AsyncIterator, List, Set, Tuple

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure

from pingslurp.config import Config
from pingslurp.podping_schemas import Podping


def get_mongo_db(collection: str = "all_podpings") -> AsyncIOMotorCollection:
    """Returns the MongoDB"""
    return AsyncIOMotorClient(Config.DB_CONNECTION)[Config.ROOT_DB_NAME][collection]


def get_mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(Config.DB_CONNECTION)[Config.ROOT_DB_NAME]


def setup_mongo_db() -> None:
    """Check if the DB exists and set it up if needed. Returns number of new DBs"""
    count = 0
    # db.all_podpings.updateMany({op_id: null}, { $set: { op_id: 1 }})
    # this command run in mongoshell to add op_id to all records 2022-11-07
    client = MongoClient(Config.DB_CONNECTION)[Config.ROOT_DB_NAME]
    collection_names = client.list_collection_names()
    if not Config.COLLECTION_NAME in collection_names:
        client.create_collection(Config.COLLECTION_NAME)
        logging.info(f"DB: {Config.COLLECTION_NAME} created in database")
    else:
        logging.info(f"DB: {Config.COLLECTION_NAME} already exists in database")

    # Check/create indexes
    client[Config.COLLECTION_NAME].create_index(
        [("trx_id", ASCENDING), ("op_id", ASCENDING)],
        name="trx_id_1_op_id_1",
        unique=True,
    )
    client[Config.COLLECTION_NAME].create_index([("block_num", ASCENDING)])
    client[Config.COLLECTION_NAME].create_index([("timestamp", DESCENDING)])
    client[Config.COLLECTION_NAME].create_index([("iris", ASCENDING)])
    try:
        client[Config.COLLECTION_NAME].drop_index(index_or_name="trx_id")
    except OperationFailure as ex:
        pass
    if not Config.COLLECTION_NAME_META in collection_names:
        # Create timeseries collection:
        client.create_collection(
            Config.COLLECTION_NAME_META,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "seconds",
            },
        )
        logging.info(f"DB: {Config.COLLECTION_NAME_META} created in database")
    else:
        logging.info(f"DB: {Config.COLLECTION_NAME_META} already exists in database")
    return


async def insert_podping(db_client: AsyncIOMotorClient, pp: Podping) -> bool:
    """Put a podping in the database, returns True if new podping was inserted
    False if duplicate."""
    data = pp.db_format()
    data_meta = pp.db_format_meta()
    try:
        ans = await db_client[Config.COLLECTION_NAME].insert_one(data)
        # if we have a new podping, store its metadata
        try:
            ans2 = await db_client[Config.COLLECTION_NAME_META].insert_one(data_meta)
            new_value = {"$set": {"stored_meta": True}}
            ans3 = await db_client[Config.COLLECTION_NAME].update_one(
                {"trx_id": pp.trx_id, "op_id": pp.op_id}, new_value
            )
        except Exception as ex:
            logging.error(ex)
    except DuplicateKeyError as ex:
        logging.debug(f"Duplicate Key: {pp.trx_id} {pp.op_id}")
        doc = await db_client[Config.COLLECTION_NAME].find_one(
            {"trx_id": pp.trx_id, "op_id": pp.op_id}
        )
        if doc and not doc.get("stored_meta"):
            ans2 = await db_client[Config.COLLECTION_NAME_META].insert_one(data_meta)
            new_value = {"$set": {"stored_meta": True}}
            ans3 = await db_client[Config.COLLECTION_NAME].update_one(
                {"trx_id": pp.trx_id}, new_value
            )
            logging.debug(f"Metadata updated for        {pp.trx_id}")
        else:
            logging.debug(f"Metadata already exists for {pp.trx_id}")
        return False

    return True


# async def store_meta()


async def block_at_postion(position=1, db: AsyncIOMotorCollection = None) -> int:
    """
    Returns the block at the given position in the database
    0 is the first block, -1 is the last block.
    """
    if db is None:
        db = get_mongo_db()
    sort_order = 1
    if position < 0:
        sort_order = -1
        position = abs(position) - 1
    cursor = db.find({}, {"block_num": 1}, allow_disk_use=True).sort(
        [("block_num", sort_order)]
    )
    i = 0
    async for doc in cursor:
        if i == position:
            return int(doc["block_num"])
        i += 1
    return 0


async def all_blocks(db: AsyncIOMotorCollection = None) -> List:
    """Return a set of all block_num currently in the database from lowest to
    highest"""
    if db is None:
        db = get_mongo_db()
    cursor = db.find({}, {"block_num": 1}, allow_disk_use=True).sort([("block_num", 1)])
    ans = list()
    async for doc in cursor:
        ans.append(doc["block_num"])
    return ans


async def all_blocks_it(db: AsyncIOMotorCollection = None) -> AsyncIterator[int]:
    """Returns iterator of set of all block_num currently in the database from lowest to
    highest"""
    if db is None:
        db = get_mongo_db()
    cursor = db.find({}, {"block_num": 1}, allow_disk_use=True).sort([("block_num", 1)])
    async for doc in cursor:
        yield doc["block_num"]


async def is_empty(iterable: AsyncIterator):
    """Returns true if the async iterator is not empty"""
    try:
        _ = await iterable.__anext__()
    except StopAsyncIteration:
        return True
    return False


async def find_big_gaps(
    block_gap_size: int = None,
    time_span: timedelta = None,
    db: AsyncIOMotorCollection = None,
) -> List[Tuple[int, int]]:
    """Find big gaps in block list greater than block_gap_size blocks or
    time_span seconds."""
    if not block_gap_size and not time_span:
        time_span = timedelta(hours=1)

    if not block_gap_size:
        block_gap_size = int(time_span.seconds / 3)
    big_gaps = []
    gap = (0, 0)
    last_block = 0
    async for range_block in range_extract(all_blocks_it(db=db)):
        if range_block[0] - last_block > block_gap_size:
            logging.debug(f"Big gap at: {range_block}")
            gap = (last_block, range_block[0] - 1)
            big_gaps.append(gap)
        last_block = max(range_block)
    return big_gaps


async def range_extract(iterable: AsyncIterator) -> AsyncIterator:
    """Assumes iterable is sorted sequentially. Returns iterator of range tuples."""
    try:
        i = await iterable.__anext__()
    except StopAsyncIteration:
        return

    while True:
        low = i

        try:
            j = await iterable.__anext__()
        except StopAsyncIteration:
            yield (low,)
            return
        while i + 1 == j:
            i_next = j
            try:
                j = await iterable.__anext__()
            except StopAsyncIteration:
                yield (low, j)
                return
            i = i_next

        hi = i

        if hi - low >= 2:
            yield (low, hi)
        elif hi - low == 1:
            yield (low,)
            yield (hi,)
        else:
            yield (low,)

        i = j


# async def blocks_of_blocks(db: AsyncIOMotorCollection = None):
#     """Returns the groups of contiguous blocks we have already searched through"""

#     data = all_blocks_it(db)

#     for k, g in groupby(enumerate(data), lambda (i,x):i-x):
#        print map(operator.itemgetter(1), g)
