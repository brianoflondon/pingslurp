import asyncio
import logging
from datetime import datetime, timedelta
from timeit import default_timer as timer
from typing import Coroutine, List, Tuple

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
    start_block = get_current_hive_block_num() - int(600 / 3)
    async with asyncio.TaskGroup() as tg:
        live_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=start_block, database_cache=0, message="LIVE"
            )
        )


async def catchup_loop():
    start_block = await block_at_postion(0) - int(
        7200 / 3
    )  # await block_at_postion(0) - 1000
    # end_block = await block_at_postion(0)
    end_block = get_current_hive_block_num()

    async with asyncio.TaskGroup() as tg:
        catchup_task = tg.create_task(
            history_loop(start_block=start_block, end_block=end_block)
        )


async def fillgaps_loop():
    time_span = timedelta(seconds=360)
    block_gaps, date_gaps = await find_date_gaps(time_span=time_span)
    date_gaps = date_gaps[1:-1]
    block_gaps = block_gaps[1:-1]
    summary = []
    async with asyncio.TaskGroup() as tg:
        for i, gap in enumerate(block_gaps):
            start, end = date_gaps[i]
            message = f"GAP {i:3}"
            logging.info(f"{message} |Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")
            found = tg.create_task(
                keep_checking_hive_stream(
                    start_block=gap[0] - 10,
                    end_block=gap[1] + 10,
                    database_cache=10,
                    message=message,
                ),
                name=message,
            )
            summary.append(found)
    for found in summary:
        logging.info(found)


def run_main_loop(task: Coroutine):
    try:
        asyncio.run(task)
    except asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised")
        logging.warning(ex)
        raise typer.Exit()
    except KeyboardInterrupt:
        logging.info("Interrupted with ctrc-C")
        raise typer.Exit()


@app.command()
def live():
    """
    Start the Pingslurp slurping up new podpings from right now
    """
    logging.info("Starting to slurp")
    setup_mongo_db()
    run_main_loop(live_loop())


@app.command()
def catchup():
    """
    Start the Pingslurp slurping and catchup from the last podping
    slurpped to the live chain, then keep checking.
    """
    logging.info("Catching up and continuing to slurp")
    setup_mongo_db()
    run_main_loop(catchup_loop())


@app.command()
def fillgaps():
    """
    Finds gaps in current database and fills them.
    """
    run_main_loop(fillgaps_loop())


if __name__ == "__main__":
    app()
    # typer.run(main)
