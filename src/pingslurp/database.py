import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncIterator, List, Tuple

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from pytz import utc
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
    """Check if the DB exists and set it up if needed."""
    try:
        db = MongoClient(Config.DB_CONNECTION)[Config.ROOT_DB_NAME]
        collection = db[Config.COLLECTION_NAME]

        # === Regular Collection Setup ===
        collection_names = db.list_collection_names()

        if Config.COLLECTION_NAME not in collection_names:
            db.create_collection(Config.COLLECTION_NAME)
            LOG.info(f"DB: {Config.COLLECTION_NAME} created")
        else:
            LOG.info(f"DB: {Config.COLLECTION_NAME} already exists")

        # --- Indexes for main collection ---

        # 1. Unique compound index
        index_name = "trx_id_1_op_id_1"
        if not any(idx["name"] == index_name for idx in collection.list_indexes()):
            collection.create_index(
                [("trx_id", ASCENDING), ("op_id", ASCENDING)],
                name=index_name,
                unique=True,
                background=True,
            )
            LOG.info(f"Created unique index: {index_name}")

        # 2. block_num index
        collection.create_index([("block_num", ASCENDING)], background=True)

        # 3. TTL index on timestamp (this replaces the old descending index)
        try:
            ttl_index_name = "timestamp_ttl"
            collection.create_index(
                [("timestamp", DESCENDING)],  # or ASCENDING - doesn't matter for TTL
                name=ttl_index_name,
                expireAfterSeconds=Config.TTL_SECONDS,  # <-- GLOBAL PARAMETER
                background=True,
            )
            LOG.info(
                f"Created TTL index '{ttl_index_name}' with expireAfterSeconds={Config.TTL_SECONDS}"
            )
        except OperationFailure as e:
            if e.code == 85:  # IndexOptionsConflict
                LOG.warning(
                    "TTL index already exists with different options. Consider dropping and recreating if needed."
                )
                # Optional: auto-fix by dropping old index (uncomment if you want aggressive fix)
                # collection.drop_index("timestamp_-1")   # or whatever the conflicting name is
                # collection.drop_index("ttl-timestamp-15820800-seconds")
            else:
                LOG.error(f"Failed to create TTL index: {e}")
                # Don't raise - let setup continue

        # 4. iris index
        collection.create_index([("iris", ASCENDING)], background=True)

        # Cleanup old leftover index if it still exists
        try:
            collection.drop_index("trx_id")
        except OperationFailure:
            pass  # index doesn't exist - that's fine

        # === Timeseries Collections ===
        for coll_name in [Config.COLLECTION_NAME_META, Config.COLLECTION_NAME_HOSTS]:
            if coll_name not in collection_names:
                db.create_collection(
                    coll_name,
                    timeseries={
                        "timeField": "timestamp",
                        "metaField": "metadata",
                        "granularity": "minutes",
                    },
                )
                LOG.info(f"Created timeseries collection: {coll_name}")
            else:
                LOG.info(f"Timeseries collection already exists: {coll_name}")

            # Optional: Add TTL to timeseries collections too (if desired)
            # try:
            #     db[coll_name].create_index(
            #         [("timestamp", ASCENDING)],
            #         expireAfterSeconds=Config.TTL_SECONDS,
            #         background=True
            #     )
            # except OperationFailure:
            #     pass

        LOG.info("MongoDB setup completed successfully")
        return

    except Exception as e:
        LOG.error(f"Critical error during MongoDB setup: {e}", exc_info=True)
        raise  # or handle gracefully depending on your startup policy


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


async def insert_podping(db_client: AsyncIOMotorClient, pp: Podping) -> PodpingDatabaseResult:
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
        # Trap error here if timeout. pymongo.errors.NetworkTimeout:
        ans = await db_client[Config.COLLECTION_NAME].insert_one(data)
        pdr.podping = True
        # if we have a new podping, store its metadata
        try:
            data_meta = pp.db_format_meta()
            meta_ts_ins = await db_client[Config.COLLECTION_NAME_META].insert_one(data_meta)
            pdr.meta = True
            data_hosts_ts = pp.db_format_hosts_ts()
            if data_hosts_ts:
                hosts_ts_ins = await db_client[Config.COLLECTION_NAME_HOSTS].insert_many(
                    [host for host in data_hosts_ts]
                )
                pdr.hosts_ts = True
            new_value = {"$set": {"stored_meta": True, "stored_hosts": True}}
            podpings_update = await db_client[Config.COLLECTION_NAME].update_one(
                {"trx_id": pp.trx_id, "op_id": pp.op_id}, new_value
            )
        except Exception as ex:
            LOG.error(ex)
    except DuplicateKeyError:
        pdr.podping = False
        LOG.debug(f"Duplicate Key: {pp.trx_id} {pp.op_id}")
        meta_result = await update_meta_ts(db_client, pp)
        pdr.hosts_ts = meta_result.hosts_ts
        pdr.meta = meta_result.meta

    return pdr


async def update_meta_ts(db_client: AsyncIOMotorClient, pp: Podping) -> PodpingDatabaseResult:
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


def gaps_from_block_bins(
    bins: List[Tuple[int, int]],
    block_gap_size: int,
) -> List[Tuple[int, int]]:
    """Gaps between inclusive block-coverage bins, including a leading gap from block 0.

    Bins are sorted by start. Each bin is at most ``block_gap_size`` blocks wide, so a
    larger gap cannot sit inside one bin.
    """
    big_gaps: List[Tuple[int, int]] = []
    last_block = 0
    for lo, hi in bins:
        if lo - last_block > block_gap_size:
            big_gaps.append((last_block, lo - 1))
        last_block = max(last_block, hi)
    return big_gaps


async def _is_timeseries(collection: AsyncIOMotorCollection) -> bool:
    cursor = await collection.database.list_collections(filter={"name": collection.name})
    async for spec in cursor:
        return "timeseries" in spec.get("options", {})
    return False


async def _has_block_num_index(collection: AsyncIOMotorCollection) -> bool:
    async for idx in collection.list_indexes():
        if list(idx["key"].items()) == [("block_num", 1)]:
            return True
    return False


def _sorted_block_num_cursor(collection: AsyncIOMotorCollection, sort_order: int, hint: bool):
    """Index-ordered block_num scan. Disk spill is disabled.

    MongoDB 8 refuses a spillable sort when the node has under 500MB free
    (OutOfDiskSpace, code 14031), even if ``allowDiskUse`` is only a fallback.
    """
    cursor = collection.find({}, {"block_num": 1}, allow_disk_use=False).sort(
        [("block_num", sort_order)]
    )
    if hint:
        cursor = cursor.hint([("block_num", ASCENDING)])
    return cursor


async def block_at_postion(position=1, collection: AsyncIOMotorCollection = None) -> int:
    """
    Returns the block at the given position in the database
    0 is the first block, -1 is the last block.
    """
    if collection is None:
        collection = get_mongo_db()
    sort_order = ASCENDING
    if position < 0:
        sort_order = DESCENDING
        position = abs(position) - 1

    if await _is_timeseries(collection):
        return await _timeseries_block_at_position(collection, position, sort_order)

    cursor = _sorted_block_num_cursor(
        collection,
        sort_order,
        hint=await _has_block_num_index(collection),
    )
    docs = await cursor.skip(position).limit(1).to_list(1)
    if not docs:
        return 0
    return int(docs[0]["block_num"])


async def _timeseries_block_at_position(
    collection: AsyncIOMotorCollection,
    position: int,
    sort_order: int,
) -> int:
    """First and last block use bucket min/max. Other positions are a top-k sort.

    Both stay in memory. A full sort of every measurement would try to spill to disk.
    """
    if position == 0:
        accumulator = "$min" if sort_order == ASCENDING else "$max"
        cursor = collection.aggregate(
            [{"$group": {"_id": None, "block_num": {accumulator: "$block_num"}}}],
            allowDiskUse=False,
        )
        docs = await cursor.to_list(1)
        if not docs or docs[0].get("block_num") is None:
            return 0
        return int(docs[0]["block_num"])

    cursor = collection.aggregate(
        [
            {"$sort": {"block_num": sort_order}},
            {"$limit": position + 1},
            {"$project": {"block_num": 1}},
        ],
        allowDiskUse=False,
    )
    docs = await cursor.to_list(length=position + 1)
    if len(docs) <= position:
        return 0
    return int(docs[position]["block_num"])


async def all_blocks(collection: AsyncIOMotorCollection = None) -> List:
    """Return a set of all block_num currently in the database from lowest to
    highest"""
    ans = []
    async for block_num in all_blocks_it(collection=collection):
        ans.append(block_num)
    return ans


async def all_blocks_it(
    collection: AsyncIOMotorCollection = None,
) -> AsyncIterator[int]:
    """Returns iterator of set of all block_num currently in the database from lowest to
    highest"""
    if collection is None:
        collection = get_mongo_db()
    cursor = _sorted_block_num_cursor(
        collection,
        ASCENDING,
        hint=await _has_block_num_index(collection),
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
    if db is None:
        db = get_mongo_db()

    # Time series measurements are not ordered by block_num. Sorting all of them
    # spills to disk, which fails when the node has under 500MB free.
    if await _is_timeseries(db):
        return await _find_big_gaps_timeseries(db, block_gap_size)

    big_gaps = []
    last_block = 0
    async for range_block in range_extract(all_blocks_it(collection=db)):
        if range_block[0] - last_block > block_gap_size:
            LOG.debug(f"Big gap at: {range_block}")
            big_gaps.append((last_block, range_block[0] - 1))
        last_block = max(range_block)
    return big_gaps


async def _find_big_gaps_timeseries(
    collection: AsyncIOMotorCollection,
    block_gap_size: int,
) -> List[Tuple[int, int]]:
    """Find gaps from block_num bins that fit in memory.

    Each bin is ``block_gap_size`` blocks wide, so a gap larger than that has to
    fall between bins. ``allowDiskUse`` stays off so mongod does not demand 500MB
    of free scratch space.
    """
    bin_width = block_gap_size if block_gap_size > 0 else 1
    LOG.info(f"Scanning {collection.name} in bins of {bin_width} blocks")
    pipeline = [
        {
            "$group": {
                "_id": {
                    "$subtract": [
                        "$block_num",
                        {"$mod": ["$block_num", bin_width]},
                    ]
                },
                "lo": {"$min": "$block_num"},
                "hi": {"$max": "$block_num"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    bins: List[Tuple[int, int]] = []
    cursor = collection.aggregate(pipeline, allowDiskUse=False)
    async for row in cursor:
        bins.append((int(row["lo"]), int(row["hi"])))
    return gaps_from_block_bins(bins, block_gap_size)


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


async def expire_old_data(days: int, check: bool = True):
    """
    Expires old data from the database
    if check is True, it will check and return the number of documents that
    that will be deleted.
    """
    date_ago = datetime.now(tz=utc) - timedelta(days=days)
    result = {}
    for col in [
        Config.COLLECTION_NAME_META,
        Config.COLLECTION_NAME_HOSTS,
        Config.COLLECTION_NAME,
    ]:
        collection = get_mongo_db(col)
        if "_ts" in col:
            filter = get_filter(metadata=True, date_ago=date_ago)
        else:
            filter = get_filter(metadata=False, date_ago=date_ago)

        # find the oldest record in the collection
        oldest = await collection.find_one({}, {"timestamp": 1}, sort=[("timestamp", 1)])
        LOG.info(f"Oldest record in {col} is {oldest['timestamp']}")
        # which how many days ago was that?
        oldest_date = oldest["timestamp"].replace(tzinfo=utc)
        days_ago = datetime.now(tz=utc) - oldest_date
        LOG.info(f"Days ago: {days_ago.days}")
        check_result = await collection.count_documents(filter)
        result[col] = check_result
        LOG.info(f"Documents to be deleted in {col}: {check_result}")
        if not check:
            try:
                delete_result = await collection.delete_many(filter)
                result[col] = delete_result
            except Exception as ex:
                LOG.error(ex)
    return result


def get_filter(metadata: bool, date_ago: datetime):
    """Returns the filter for the database query"""
    if metadata:
        filter = {"metadata.timestamp": {"$lt": date_ago}}
    else:
        filter = {"timestamp": {"$lt": date_ago}}
    return filter


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
