import asyncio
import logging
from datetime import datetime, timedelta
from timeit import default_timer as timer
from typing import List, Tuple

import typer
from motor.motor_asyncio import AsyncIOMotorCollection
from rich import print

from pingslurp.config import Config
from pingslurp.database import (
    all_blocks,
    all_blocks_it,
    block_at_postion,
    find_big_gaps,
    get_mongo_db,
    is_empty,
    setup_mongo_db,
)
from pingslurp.hive_calls import (
    HiveConnectionError,
    get_block_datetime,
    get_current_hive_block_num,
    keep_checking_hive_stream,
)
from pingslurp.podping import Podping

app = typer.Typer(help="Slurping up Podpings with Pingslurp")


async def find_date_gaps(
    block_gap_size: int = None,
    time_span: timedelta = None,
    db: AsyncIOMotorCollection = None,
) -> Tuple[List[Tuple[int, int]], List[Tuple[datetime, datetime]]]:
    block_gaps = await find_big_gaps(
        block_gap_size=block_gap_size, time_span=time_span, db=db
    )
    last_block = await block_at_postion(position=-1, db=db)
    current_block = get_current_hive_block_num()
    block_gaps.append((last_block, current_block))
    date_gaps = []
    for start, end in block_gaps:
        start = get_block_datetime(start)
        end = get_block_datetime(end)
        date_gaps.append((start, end))
    return block_gaps, date_gaps


async def setup_check_database():
    """Check if we have a database and return stuff"""
    setup_mongo_db()
    print(f"Using database at {Config.DB_CONNECTION}")
    empty = await is_empty(all_blocks_it())
    if empty:
        print("Databse is empty")
    else:
        col_block_gaps = {}
        for col in [Config.COLLECTION_NAME, Config.COLLECTION_NAME_META]:
            db = get_mongo_db(collection=col)
            block_gaps, date_gaps = await find_date_gaps(db=db)
            col_block_gaps[col] = block_gaps
            print(f"Database Collection {col} Gaps:")
            for start, end in date_gaps:
                print(f"Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")


@app.command()
def check():
    """
    Check Pingslurp
    """
    debug = False
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(module)-14s %(lineno) 5d : %(message)s",
        datefmt="%m-%dT%H:%M:%S",
    )

    asyncio.run(setup_check_database())
    raise typer.Exit()


async def history_loop(start_block: int, end_block: int):
    async with asyncio.TaskGroup() as tg:
        history_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=start_block,
                end_block=end_block,
                database_cache=10,
                message="HIST",
            ),
            name="history_task",
        )


async def live_loop():
    start_block = get_current_hive_block_num() - 20
    while True:
        async with asyncio.TaskGroup() as tg:
            try:
                live_task = tg.create_task(
                    keep_checking_hive_stream(
                        start_block=start_block, database_cache=0, message="LIVE"
                    )
                )
            except HiveConnectionError:
                start_block = await block_at_postion(-1) - 20
                pass


async def catchup_loop():
    start_block = 69329927 # await block_at_postion(0) - 1000
    end_block = await block_at_postion(0)
    end_block = get_current_hive_block_num()

    async with asyncio.TaskGroup() as tg:
        catchup_task = tg.create_task(
            history_loop(start_block=start_block, end_block=end_block)
        )


@app.command()
def live():
    """
    Start the Pingslurp slurping up new podpings from right now
    """
    logging.info("Starting to slurp")
    setup_mongo_db()
    try:
        asyncio.run(live_loop())
    except asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised")
        logging.warning(ex)
        raise typer.Exit()
    except KeyboardInterrupt:
        logging.info("Interrupted with ctrc-C")
        raise typer.Exit()


@app.command()
def catchup():
    """
    Start the Pingslurp slurping and catchup from the last podping
    slurpped to the live chain, then keep checking.
    """
    logging.info("Catching up and continuing to slurp")
    setup_mongo_db()
    try:
        asyncio.run(catchup_loop())
        # live()
    except asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised")
        logging.warning(ex)
        raise typer.Exit()
    except KeyboardInterrupt:
        logging.info("Interrupted with ctrc-C")
        raise typer.Exit()


if __name__ == "__main__":
    app()
    # typer.run(main)
