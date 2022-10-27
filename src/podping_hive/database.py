from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

from podping_hive.podping import Podping

DB_CONNECTION = "mongodb://localhost:27017"


def get_mongo_db(collection: str) -> AsyncIOMotorCollection:
    """Returns the MongoDB"""
    return AsyncIOMotorClient(DB_CONNECTION)["podping"][collection]


async def insert_podping(database: AsyncIOMotorCollection, pp: Podping):
    """Put a podping in the database, does nothing if duplicate"""
    data = pp.dict()
    try:
        await database.insert_one(data)
    except DuplicateKeyError:
        pass
