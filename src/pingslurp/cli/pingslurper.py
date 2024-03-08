import asyncio
import logging
import sys
from datetime import datetime, timedelta
from timeit import default_timer as timer
from typing import Coroutine, List, Optional, Tuple

import typer
from motor.motor_asyncio import AsyncIOMotorCollection
from rich import print
from tqdm import trange
from tqdm.contrib.logging import logging_redirect_tqdm

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


async def setup_check_database(time_span: timedelta = None):
    """Check if we have a database and return stuff"""
    setup_mongo_db()
    time_span = timedelta(seconds=360) if not time_span else time_span
    LOG.info(f"Using database at {Config.DB_CONNECTION}")
    empty = await is_empty(all_blocks_it())
    if empty:
        print("Databse is empty")
    else:
        col_block_gaps = {}
        for col in [Config.COLLECTION_NAME, Config.COLLECTION_NAME_META]:
            db = get_mongo_db(collection=col)
            block_gaps, date_gaps = await find_date_gaps(time_span=time_span, db=db)
            col_block_gaps[col] = block_gaps
            print(f"Database Collection {col} Gaps:")
            for start, end in date_gaps:
                print(f"Date gap: {start:%d-%m-%Y} ->  {end:%d-%m-%Y} | {end - start}")


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
    LOG.info(history_task.result())


async def live_loop():
    """Starts scanning 10 mins before the most recent block in the database"""
    start_block = await block_at_postion(-1) - int(360 / 3)
    async with asyncio.TaskGroup() as tg:
        live_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=start_block,
                database_cache=0,
                message="LIVE",
                state_options=state_options,
            )
        )
    LOG.info(live_task.result())


async def catchup_loop():
    """
    Starts scanning Live and looks back 30 mins before the most recent block
    in the database
    """
    start_block = await block_at_postion(-1) - int(1800 / 3)
    end_block = get_current_hive_block_num()
    live_start_block = get_current_hive_block_num()

    async with asyncio.TaskGroup() as tg:
        live_task = tg.create_task(
            keep_checking_hive_stream(
                start_block=live_start_block,
                database_cache=0,
                message="LIVE",
                state_options=state_options,
            )
        )
        catchup_task = tg.create_task(
            history_loop(start_block=start_block, end_block=end_block)
        )
    LOG.info(catchup_task.result())
    LOG.info(live_task.result())


async def fillgaps_loop(
    block_gaps: List[Tuple[int, int]] = None, time_span: timedelta = None
):
    time_span = timedelta(seconds=360) if not time_span else time_span
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
            LOG.info(
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
        LOG.info(found.result())


async def scan_history_loop(start_days: float, bots: int = 20, end_days: float = None):
    """Use to fillin the history before the current database."""
    time_delta = timedelta(days=start_days)
    start_block = get_block_num(time_delta=time_delta)
    if end_days is None:
        end_block = await block_at_postion(0) + 6
    else:
        end_time_delta = timedelta(days=end_days)
        end_block = get_block_num(time_delta=end_time_delta)
    if start_block > end_block:
        LOG.info("Aborting: trying to start history scan within the current database.")
        return
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
        block_gaps.append((int(start_block - 50), next_end))
        start_block += block_gap

    LOG.info(block_gaps)
    await fillgaps_loop(block_gaps=block_gaps)


def run_main_loop(task: Coroutine):
    try:
        start_time = timer()
        LOG.info("Starting to scan")
        asyncio.run(task)
    except* asyncio.CancelledError as ex:
        LOG.warning("asyncio.CancelledError raised")
        LOG.warning(ex)
        raise typer.Exit()
    except* KeyboardInterrupt:
        LOG.info("Interrupted with ctrc-C")
        raise typer.Exit()
    finally:
        LOG.info(f"Total time: {timer()-start_time:.2f}s")


@app.callback()
def main(
    verbose: bool = typer.Option(
        False, help="Show verbose output including logging every ping"
    ),
    just_iris: bool = typer.Option(
        False, help="""Just show the IRIs from the pingslurp database.
        Sends stream of iris to the `stdout`
        Diagnostic information is sent to `stderr`"""
    ),
):
    """
    Manage users in the awesome CLI app.
    """
    state_options.verbose = verbose
    state_options.just_iris = just_iris

    if verbose:
        LOG.info("Using Verbose output")
    setup_mongo_db()


@app.command()
def live():
    """
    Start the Pingslurp slurping up new podpings from right now
    """
    LOG.info("Starting to slurp")
    run_main_loop(live_loop())


@app.command()
def catchup():
    """
    Start the Pingslurp slurping and catchup from the last podping
    slurpped to the live chain, then keep checking.
    """
    LOG.info("Catching up and continuing to slurp")
    run_main_loop(catchup_loop())


@app.command()
def fillgaps(
    time_span: Optional[float] = typer.Option(
        60, help="Minimum gaps size in mins to consider as a gap (default 60 mins)"
    ),
):
    """
    Finds gaps in current database and fills them.
    """
    td_time_span = timedelta(seconds=time_span * 60)
    run_main_loop(fillgaps_loop(time_span=td_time_span))


@app.command()
def check(
    time_span: Optional[float] = typer.Option(
        60, help="Minimum gaps size in minutes to consider as a gap (default 60 mins)"
    ),
):
    """
    Check Pingslurp
    """
    td_time_span = timedelta(seconds=time_span * 60)
    asyncio.run(setup_check_database(time_span=td_time_span))
    raise typer.Exit()


@app.command()
def scanrange(
    start_days: Optional[float] = typer.Option(
        5, help="Days back to start scanning from"
    ),
    end_days: Optional[float] = typer.Option(
        0, help="Days backward to end scanning on"
    ),
    bots: Optional[float] = typer.Option(5, help="Number of scanning bots to run"),
):
    """
    Scans a range of blocks or dates
    """
    run_main_loop(scan_history_loop(start_days, bots, end_days=end_days))


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


LOG = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stderr)
LOG.addHandler(handler)

if __name__ == "__main__":
    with logging_redirect_tqdm():
        LOG.info(f"Starting up Pingslurper version {__version__}")
        app()

# typer.run(main)
