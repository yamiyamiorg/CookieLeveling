import discord

from cookieleveling.config.config import Config
from cookieleveling.usecases.hourly_tick import run_hourly_tick
from cookieleveling.usecases.minute_tick import run_minute_tick


def run_minute_tasks(bot: discord.Client, config: Config) -> int:
    return run_minute_tick(bot, config)


async def run_hourly_tasks(bot: discord.Client, config: Config) -> None:
    await run_hourly_tick(bot, config)
