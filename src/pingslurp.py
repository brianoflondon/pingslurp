import asyncio
import logging
import sys
from datetime import datetime, timedelta
from timeit import default_timer as timer
from typing import Coroutine, List, Optional, Tuple

import typer
from motor.motor_asyncio import AsyncIOMotorCollection
from rich import print

from pingslurp import __version__
from pingslurp.config import Config, StateOptions
from pingslurp.database import (
    all_blocks_it,
    block_at_postion,
    database_update,
    find_big_gaps,
    get_mongo_db,
    is_empty,
    setup_mongo_db,
)
from pingslurp.hive_calls import (
    get_block_datetime,
    get_block_num,
    get_current_hive_block_num,
    keep_checking_hive_stream,
)

app = typer.Typer(help="Slurping up Podpings with Pingslurp")
DATABASE_CACHE = 100

state_options = StateOptions()


async def find_date_gaps(
    block_gap_size: int = None,
    time_span: timedelta = None,
    db: AsyncIOMotorCollection = None,
) -> Tuple[List[Tuple[int, int]], List[Tuple[datetime, datetime]]]:
    block_gaps = await find_big_gaps(
        block_gap_size=block_gap_size, time_span=time_span, db=db
    )
    last_block = await block_at_postion(position=-1, collection=db)
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
    logging.info(f"Using database at {Config.DB_CONNECTION}")
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


async def history_loop(
    start_block: Optional[int] = None,
    time_delta: Optional[timedelta] = None,
    end_block: Optional[int] = sys.maxsize,
):
    async with asyncio.TaskGroup() as tg:
        history_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=start_block,
                end_block=end_block,
                time_delta=time_delta,
                database_cache=DATABASE_CACHE,
                message="HIST",
                state_options=state_options,
            ),
            name="history_task",
        )
    logging.info(history_task.result())


async def live_loop():
    start_block = await block_at_postion(-1) - 600
    async with asyncio.TaskGroup() as tg:
        live_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=start_block,
                database_cache=0,
                message="LIVE",
                state_options=state_options,
            )
        )
    logging.info(live_task.result())


async def catchup_loop():
    start_block = await block_at_postion(-1) - int(3600 / 3)
    end_block = get_current_hive_block_num()
    live_start_block = await block_at_postion(-1)

    async with asyncio.TaskGroup() as tg:
        catchup_task = tg.create_task(
            history_loop(start_block=start_block, end_block=end_block)
        )
        live_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=live_start_block,
                database_cache=0,
                message="LIVE",
                state_options=state_options,
            )
        )
    logging.info(catchup_task.result())
    logging.info(live_task.result())


async def fillgaps_loop(block_gaps: List[Tuple[int, int]] = None):
    time_span = timedelta(seconds=360)
    if not block_gaps:
        block_gaps, date_gaps = await find_date_gaps(time_span=time_span)
        date_gaps = date_gaps[1:-1]
        block_gaps = block_gaps[1:-1]
    else:
        date_gaps = []
        for start, end in block_gaps:
            start_date = get_block_datetime(start)
            end_date = get_block_datetime(end)
            date_gaps.append((start_date, end_date))
    summary = []
    async with asyncio.TaskGroup() as tg:
        for i, gap in enumerate(block_gaps):
            start, end = date_gaps[i]
            message = f"GAP {i:3}"
            logging.info(
                f"{message} |Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}"
            )
            found = tg.create_task(
                keep_checking_hive_stream(
                    start_block=gap[0] - 10,
                    end_block=gap[1] + 10,
                    database_cache=DATABASE_CACHE,
                    message=message,
                    state_options=state_options,
                ),
                name=message,
            )
            summary.append(found)
    for found in summary:
        logging.info(found.result())


async def scan_history_loop(start_days, bots):
    time_delta = timedelta(days=start_days)
    start_block = get_block_num(time_delta=time_delta)
    end_block = await block_at_postion(0) + 6
    current_block = get_current_hive_block_num()
    blocks_per_day = (24 * 60 * 60) / 3
    total_blocks = end_block - start_block
    block_gap = total_blocks / bots
    block_gaps = []
    next_end = 0
    while start_block < end_block and next_end < current_block:
        next_end = int(start_block + block_gap + 10)
        if next_end > current_block:
            next_end = current_block
        block_gaps.append((int(start_block - 50),next_end))
        start_block += block_gap

    print(block_gaps)
    await fillgaps_loop(block_gaps=block_gaps)


def run_main_loop(task: Coroutine):
    try:
        start_time = timer()
        logging.info("Starting to scan")
        asyncio.run(task)
    except* asyncio.CancelledError as ex:
        logging.warning("asyncio.CancelledError raised")
        logging.warning(ex)
        raise typer.Exit()
    except* KeyboardInterrupt:
        logging.info("Interrupted with ctrc-C")
        raise typer.Exit()
    finally:
        logging.info(f"Total time: {timer()-start_time:.2f}s")


@app.callback()
def main(verbose: bool = True):
    """
    Manage users in the awesome CLI app.
    """
    if not verbose:
        logging.info("Will not write verbose output")
        state_options.verbose = False


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


@app.command()
def scanrange(
    # start_block: Optional[int] = typer.Option(None, help="Starting block"),
    # end_block: Optional[int]= typer.Option(None, help="Ending block"),
    # start_date: Optional[datetime]=typer.Option(None, help="Date to start scanning from"),
    # end_date: Optional[datetime]=typer.Option(None, help="Date to end scanning on"),
    start_days: Optional[float] = typer.Option(
        5, help="Days back to start scanning from"
    ),
    end_days: Optional[float] = typer.Option(
        0, help="Days backward to end scanning on"
    ),
):
    """
    Scans a range of blocks or dates
    """
    time_delta = timedelta(days=start_days)
    time_delta_end = timedelta(days=end_days)
    end_block = get_block_num(time_delta=time_delta_end)
    run_main_loop(history_loop(time_delta=time_delta, end_block=end_block))


@app.command()
def scanblocks(
    start_block: Optional[int] = typer.Option(None, help="Starting block"),
    end_block: Optional[int] = typer.Option(None, help="Ending block"),
):
    """
    Scans a range of blocks
    """
    run_main_loop(history_loop(start_block=start_block, end_block=end_block))


@app.command()
def scanhistory(
    start_days: Optional[float] = typer.Option(
        90, help="Days back from now to start scanning from"
    ),
    bots: Optional[float] = typer.Option(5, help="Number of scanning bots to run"),
):
    """
    Goes back to <start_days> and starts scanning from that point to the earliest point in
    the database. Divides the scan ranges up into periods of 5 days.
    """
    # Block to end the scan at.
    run_main_loop(scan_history_loop(start_days, bots))


@app.command()
def databaseupdate(
    force_update: Optional[bool] = typer.Option(
        False, help="Delete and overwriter the timeseries collections"
    )
):
    """
    Goes through the entire database makes sure the meta data timeseries collections are
    complete with a record for each podping. Does not fetch new Podpings from Hive.
    """
    if force_update:
        are_you_sure = typer.confirm("Are you sure you want to delete all metadata?")
        if not are_you_sure:
            raise typer.Abort()

    run_main_loop(database_update(state_options, force_update))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARN
    )
    logging.info(f"Starting up Pingslurp version {__version__}")
    setup_mongo_db()
    app()
# typer.run(main)
