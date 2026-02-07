import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord

from .config import Config
from .task_runner import run_hourly_tasks, run_minute_tasks

_LOGGER = logging.getLogger(__name__)


def start_minute_scheduler(bot: discord.Client, config: Config) -> asyncio.Task:
    return bot.loop.create_task(_minute_loop(bot, config))


def start_hourly_scheduler(bot: discord.Client, config: Config) -> asyncio.Task:
    return bot.loop.create_task(_hourly_scheduler(bot, config))


async def _minute_loop(bot: discord.Client, config: Config) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            run_minute_tasks(bot, config)
        except Exception:
            _LOGGER.exception("minute tick failed")
        await asyncio.sleep(_seconds_until_next_minute())


async def _hourly_scheduler(bot: discord.Client, config: Config) -> None:
    await bot.wait_until_ready()
    if bot.get_guild(config.guild_id) is None:
        _LOGGER.warning(
            "hourly scheduler disabled: guild not found: %s", config.guild_id
        )
        return
    _schedule_next_hourly_tick(bot, config)


def _seconds_until_next_minute() -> float:
    now = time.time()
    return max(1.0, 60.0 - (now % 60.0))


def _schedule_next_hourly_tick(bot: discord.Client, config: Config) -> None:
    if bot.is_closed():
        return
    loop = bot.loop
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    next_boundary = now_jst.replace(minute=0, second=0, microsecond=0)
    if now_jst >= next_boundary:
        next_boundary += timedelta(hours=1)
    next_boundary_utc = next_boundary.astimezone(timezone.utc)
    now_utc = datetime.now(timezone.utc)
    delay_seconds = max(0.0, (next_boundary_utc - now_utc).total_seconds())
    _LOGGER.info(
        "hourly schedule next boundary: %s (in %.2fs)",
        next_boundary.isoformat(),
        delay_seconds,
    )
    handle = loop.call_at(
        loop.time() + delay_seconds, _hourly_tick_callback, bot, config
    )
    bot._hourly_handle = handle


def _hourly_tick_callback(bot: discord.Client, config: Config) -> None:
    if bot.is_closed():
        return
    task = bot.loop.create_task(_run_hourly_tick(bot, config))
    bot._hourly_tick_task = task


async def _run_hourly_tick(bot: discord.Client, config: Config) -> None:
    tick_started = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat()
    _LOGGER.info("hourly tick start: %s", tick_started)
    try:
        await run_hourly_tasks(bot, config)
    except Exception:
        _LOGGER.exception("hourly rankboard update failed")
    finally:
        tick_finished = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat()
        _LOGGER.info("hourly tick end: %s", tick_finished)
        _schedule_next_hourly_tick(bot, config)
