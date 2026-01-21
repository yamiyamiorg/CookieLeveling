import logging

import discord

from .commands import setup_commands
from .config import Config
from .db import init_db
from .scheduler import start_minute_scheduler
from .voice_tracker import handle_voice_state_update, restore_voice_state

_LOGGER = logging.getLogger(__name__)


class CookieLevelingBot(discord.Client):
    def __init__(self, config: Config) -> None:
        intents = discord.Intents.default()
        intents.voice_states = True
        super().__init__(intents=intents)
        self.config = config
        self.tree = discord.app_commands.CommandTree(self)
        self._vc_restored = False

    async def setup_hook(self) -> None:
        init_db(self.config)
        setup_commands(self, self.config)
        await self.tree.sync(guild=discord.Object(id=self.config.guild_id))
        self._minute_task = start_minute_scheduler(self, self.config)

    async def on_ready(self) -> None:
        if not self._vc_restored:
            guild = self.get_guild(self.config.guild_id)
            if guild is not None:
                restore_voice_state(guild)
                self._vc_restored = True
        _LOGGER.info("ready: %s", self.user)

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.guild.id != self.config.guild_id:
            return
        handle_voice_state_update(member.guild.id, member, before, after)
