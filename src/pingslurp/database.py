import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator, List, Set, Tuple

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from tqdm.asyncio import tqdm

from pingslurp.config import Config, StateOptions
from pingslurp.podping_schemas import Podping

LOG = logging.getLogger(__name__)


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
        LOG.info(f"DB: {Config.COLLECTION_NAME} created in database")
    else:
        LOG.info(f"DB: {Config.COLLECTION_NAME} already exists in database")

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
                "granularity": "minutes",
            },
        )
        LOG.info(f"DB: {Config.COLLECTION_NAME_META} created in database")
    else:
        LOG.info(f"DB: {Config.COLLECTION_NAME_META} already exists in database")
    if not Config.COLLECTION_NAME_HOSTS in collection_names:
        # Create timeseries collection:
        client.create_collection(
            Config.COLLECTION_NAME_HOSTS,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "minutes",
            },
        )
        LOG.info(f"DB: {Config.COLLECTION_NAME_HOSTS} created in database")
    else:
        LOG.info(f"DB: {Config.COLLECTION_NAME_HOSTS} already exists in database")

    return


@dataclass
class PodpingDatabaseResult:
    podping: bool = False
    meta: bool = False
    hosts_ts: bool = False
    trx_id: str = None
    block_num: str = None
    op_id: int = 1
    required_posting_auths: str = None

    @property
    def insert_result(self) -> str:
        p_txt = "NEW Podping" if self.podping else "DUPLICATE Podping"
        m_txt = "✅" if self.meta else "🅾️"
        h_txt = "✅" if self.hosts_ts else "🅾️"

        return (
            f"{p_txt:<18} | "
            f"Md {m_txt:<1} | "
            f"Hs {h_txt:<1} | "
            f"{self.trx_id} | {self.block_num} | {self.required_posting_auths}"
        )


async def insert_podping(
    db_client: AsyncIOMotorClient, pp: Podping
) -> PodpingDatabaseResult:
    """Put a podping in the database, returns True if new podping was inserted
    False if duplicate."""
    data = pp.db_format()
    pdr = PodpingDatabaseResult(
        trx_id=pp.trx_id,
        block_num=pp.block_num,
        op_id=pp.op_id,
        required_posting_auths=pp.required_posting_auths,
    )
    try:
        ans = await db_client[Config.COLLECTION_NAME].insert_one(data)
        pdr.podping = True
        # if we have a new podping, store its metadata
        try:
            data_meta = pp.db_format_meta()
            meta_ts_ins = await db_client[Config.COLLECTION_NAME_META].insert_one(
                data_meta
            )
            pdr.meta = True
            data_hosts_ts = pp.db_format_hosts_ts()
            if data_hosts_ts:
                hosts_ts_ins = await db_client[
                    Config.COLLECTION_NAME_HOSTS
                ].insert_many([host for host in data_hosts_ts])
                pdr.hosts_ts = True
            new_value = {"$set": {"stored_meta": True, "stored_hosts": True}}
            podpings_update = await db_client[Config.COLLECTION_NAME].update_one(
                {"trx_id": pp.trx_id, "op_id": pp.op_id}, new_value
            )
        except Exception as ex:
            LOG.error(ex)
    except DuplicateKeyError as ex:
        pdr.podping = False
        LOG.debug(f"Duplicate Key: {pp.trx_id} {pp.op_id}")
        meta_result = await update_meta_ts(db_client, pp)
        pdr.hosts_ts = meta_result.hosts_ts
        pdr.meta = meta_result.meta

    return pdr


async def update_meta_ts(
    db_client: AsyncIOMotorClient, pp: Podping
) -> PodpingDatabaseResult:
    """Run the update to meta timeseries if we are adding to existing database"""
    doc = await db_client[Config.COLLECTION_NAME].find_one(
        {"trx_id": pp.trx_id, "op_id": pp.op_id}, {"stored_meta": 1, "stored_hosts": 1}
    )
    pdr = PodpingDatabaseResult(
        trx_id=pp.trx_id,
        block_num=pp.block_num,
        op_id=pp.op_id,
        required_posting_auths=pp.required_posting_auths,
    )
    if doc and not doc.get("stored_meta"):
        data_meta = pp.db_format_meta()
        ans2 = await db_client[Config.COLLECTION_NAME_META].insert_one(data_meta)
        new_value = {"$set": {"stored_meta": True}}
        ans3 = await db_client[Config.COLLECTION_NAME].update_one(
            {"trx_id": pp.trx_id, "op_id": pp.op_id}, new_value
        )
        LOG.debug(f"Metadata updated for        {pp.trx_id}")
        pdr.meta = True
    if doc and not doc.get("stored_hosts"):
        data_hosts_ts = pp.db_format_hosts_ts()
        if data_hosts_ts:
            ans2 = await db_client[Config.COLLECTION_NAME_HOSTS].insert_many(
                [host for host in data_hosts_ts]
            )
            new_value = {"$set": {"stored_hosts": True}}
            ans3 = await db_client[Config.COLLECTION_NAME].update_one(
                {"trx_id": pp.trx_id, "op_id": pp.op_id}, new_value
            )
            LOG.debug(f"Hosts   updated for        {pp.trx_id}")
            pdr.hosts_ts = True

    return pdr


async def block_at_postion(
    position=1, collection: AsyncIOMotorCollection = None
) -> int:
    """
    Returns the block at the given position in the database
    0 is the first block, -1 is the last block.
    """
    if collection is None:
        collection = get_mongo_db()
    sort_order = 1
    if position < 0:
        sort_order = -1
        position = abs(position) - 1
    cursor = collection.find({}, {"block_num": 1}, allow_disk_use=True).sort(
        [("block_num", sort_order)]
    )
    i = 0
    async for doc in cursor:
        if i == position:
            return int(doc["block_num"])
        i += 1
    return 0


async def all_blocks(collection: AsyncIOMotorCollection = None) -> List:
    """Return a set of all block_num currently in the database from lowest to
    highest"""
    if collection is None:
        collection = get_mongo_db()
    cursor = collection.find({}, {"block_num": 1}, allow_disk_use=True).sort(
        [("block_num", 1)]
    )
    ans = list()
    async for doc in cursor:
        ans.append(doc["block_num"])
    return ans


async def all_blocks_it(
    collection: AsyncIOMotorCollection = None,
) -> AsyncIterator[int]:
    """Returns iterator of set of all block_num currently in the database from lowest to
    highest"""
    if collection is None:
        collection = get_mongo_db()
    cursor = collection.find({}, {"block_num": 1}, allow_disk_use=True).sort(
        [("block_num", 1)]
    )
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
    async for range_block in range_extract(all_blocks_it(collection=db)):
        if range_block[0] - last_block > block_gap_size:
            LOG.debug(f"Big gap at: {range_block}")
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


async def clear_meta_data():
    """Deletes ALL METADATA tables and resets the flags on all entries in the all_podpings
    table"""
    client = get_mongo_client()
    collection = get_mongo_db(Config.COLLECTION_NAME)
    filter = {"$or": [{"stored_meta": True}, {"stored_hosts": True}]}
    update = {"$unset": {"stored_meta": "", "stored_hosts": ""}}
    await collection.update_many(filter, update)

    await client.drop_collection(Config.COLLECTION_NAME_HOSTS)
    await client.drop_collection(Config.COLLECTION_NAME_META)
    setup_mongo_db()


async def database_update(state_options: StateOptions, force_update: bool = False):
    """Updates the Database scanning for all missing metadata and filling it in"""
    if force_update:
        await clear_meta_data()
    client = get_mongo_client()
    collection = client[Config.COLLECTION_NAME]
    filter = {"$or": [{"stored_meta": None}, {"stored_hosts": None}]}
    cursor = collection.find(filter, {"_id": 0})
    total = await collection.count_documents(filter)
    message = "DATABASE"
    tasks = []
    try:
        with tqdm(cursor) as pbar:
            pbar.total = total
            async for doc in pbar:
                pp = Podping.parse_obj(doc)
                if pp.iris:
                    pbar.desc = f"Scanned up to {pp.block_num}"
                    tasks.append(insert_podping(db_client=client, pp=pp))
                if len(tasks) > 1000:
                    all_pdr = await asyncio.gather(*tasks)
                    if state_options.verbose:
                        for pdr in all_pdr:
                            LOG.info(f"{message:>8} {pdr.insert_result}")
                    else:
                        pass
                        # LOG.info(f"Scanned up to {pp.block_num}")
                    tasks = []
    except asyncio.CancelledError as ex:
        LOG.warning("asyncio.CancelledError raised in database_update")
        LOG.warning(f"{ex} {ex.__class__}")
        raise ex
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    finally:
        try:
            all_pdr = await asyncio.gather(*tasks)
            if state_options.verbose:
                for pdr in all_pdr:
                    LOG.info(f"{message:>8} {pdr.insert_result}")
            else:
                LOG.info(f"Scanned up to {pp.block_num}")
            tasks = []
        except Exception as ex:
            LOG.warning(ex)
