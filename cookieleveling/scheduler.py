import asyncio
import logging
import time

import discord

from .config import Config
from .rankboard_publisher import update_rankboard
from .xp_engine import tick_minute

_LOGGER = logging.getLogger(__name__)


def start_minute_scheduler(bot: discord.Client, config: Config) -> asyncio.Task:
    return bot.loop.create_task(_minute_loop(bot, config))


def start_hourly_scheduler(bot: discord.Client, config: Config) -> asyncio.Task:
    return bot.loop.create_task(_hourly_loop(bot, config))


async def _minute_loop(bot: discord.Client, config: Config) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            updated = tick_minute(config.guild_id)
            if updated:
                _LOGGER.info("minute tick updated %s users", updated)
        except Exception:
            _LOGGER.exception("minute tick failed")
        await asyncio.sleep(_seconds_until_next_minute())


async def _hourly_loop(bot: discord.Client, config: Config) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            updated = await update_rankboard(bot, config)
            if updated:
                _LOGGER.info("hourly rankboard updated")
        except Exception:
            _LOGGER.exception("hourly rankboard update failed")
        await asyncio.sleep(_seconds_until_next_hour())


def _seconds_until_next_minute() -> float:
    now = time.time()
    return max(1.0, 60.0 - (now % 60.0))


def _seconds_until_next_hour() -> float:
    now = time.time()
    return max(1.0, 3600.0 - (now % 3600.0))
