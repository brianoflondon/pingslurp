from itertools import groupby
from typing import AsyncIterator, Generator, Set

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

from podping_hive.podping import Podping

DB_CONNECTION = "mongodb://localhost:27017"


def get_mongo_db(collection: str = "all_podpings") -> AsyncIOMotorCollection:
    """Returns the MongoDB"""
    return AsyncIOMotorClient(DB_CONNECTION)["podping"][collection]


async def insert_podping(db: AsyncIOMotorCollection, pp: Podping) -> bool:
    """Put a podping in the database, returns True if new podping was inserted
    False if duplicate."""
    data = pp.dict()
    try:
        await db.insert_one(data)
        return True
    except DuplicateKeyError:
        return False


async def block_at_postion(position=1, db: AsyncIOMotorCollection = None) -> int:
    if not db:
        db = get_mongo_db()
    sort_order = 1
    if position < 0:
        sort_order = -1
        position = abs(position) - 1
    cursor = db.find({}, {"block_num": 1}).sort([("block_num", sort_order)])
    i = 0
    async for doc in cursor:
        if i == position:
            return doc["block_num"]
        i += 1
    return 0


async def all_blocks(db: AsyncIOMotorCollection = None) -> Set:
    """Return a set of all block_num currently in the database from lowest to
    highest"""
    if not db:
        db = get_mongo_db()
    cursor = db.find({}, {"block_num": 1}).sort([("block_num", 1)])
    ans = set()
    async for doc in cursor:
        ans.add(doc["block_num"])
    return ans


async def all_blocks_it(db: AsyncIOMotorCollection = None) -> AsyncIterator[int]:
    """Returns iterator of set of all block_num currently in the database from lowest to
    highest"""
    if not db:
        db = get_mongo_db()
    cursor = db.find({}, {"block_num": 1}).sort([("block_num", 1)])
    async for doc in cursor:
        yield doc["block_num"]


async def range_extract(iterable: AsyncIterator) -> AsyncIterator:
    """Assumes iterable is sorted sequentially. Returns iterator of range tuples."""
    try:
        i = await iterable.__anext__()
    except StopIteration:
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
