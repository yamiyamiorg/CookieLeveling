import logging

import discord

from .commands import setup_commands
from .config import Config
from .db import init_db

_LOGGER = logging.getLogger(__name__)


class CookieLevelingBot(discord.Client):
    def __init__(self, config: Config) -> None:
        intents = discord.Intents.default()
        intents.voice_states = True
        super().__init__(intents=intents)
        self.config = config
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        init_db(self.config)
        setup_commands(self, self.config)
        await self.tree.sync(guild=discord.Object(id=self.config.guild_id))

    async def on_ready(self) -> None:
        _LOGGER.info("ready: %s", self.user)
