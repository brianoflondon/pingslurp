import os
from typing import Iterator
from pydantic import BaseModel
from datetime import datetime
import pymssql

class HiveSQLPodping(BaseModel):
    trx_id: str
    block_num: int
    timestamp: datetime
    trx_num: int

row = {
    "ID": 1708015122,
    "tid": "pp_video_update",
    "json": '{"version":"1.0","medium":"video","reason":"update","iris":["https://3speak.tv/rss/hawks21.xml"]}',
    "timestamp": datetime.datetime(2022, 10, 25, 16, 6, 18),
    "required_posting_auth": "podping.spk",
    "required_auth": None,
    "required_posting_auths": '["podping.spk"]',
    "required_auths": None,
    "tx_id": 2765736937,
}


def hive_sql_podping(SQLStatement: str) -> Iterator[dict]:
    db = os.environ["HIVESQL"].split()
    with pymssql.connect(
        server=db[0],
        user=db[1],
        password=db[2],
        database=db[3],
        timeout=0,
        login_timeout=10,
    ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(SQLStatement)
            for row in cursor:
                yield row


def hive_sql(SQLCommand, limit):
    db = os.environ["HIVESQL"].split()
    conn = pymssql.connect(
        server=db[0],
        user=db[1],
        password=db[2],
        database=db[3],
        timeout=0,
        login_timeout=10,
    )
    cursor = conn.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchmany(limit)
    conn.close()
    return result
