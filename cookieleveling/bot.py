import logging

import discord
from discord import app_commands

from .commands import setup_commands
from .config import Config
from .db import init_db
from .scheduler import start_hourly_scheduler, start_minute_scheduler
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
        self._register_tree_error_handler()

    def _register_tree_error_handler(self) -> None:
        @self.tree.error
        async def on_app_command_error(
            interaction: discord.Interaction, error: app_commands.AppCommandError
        ) -> None:
            if isinstance(error, app_commands.CommandNotFound):
                command_name = interaction.command.name if interaction.command else "unknown"
                _LOGGER.info("app command not found: %s", command_name)
                message = "コマンド同期中です。数秒後に再実行してください。"
            else:
                message = "処理に失敗しました（ログ参照）"
                _LOGGER.exception("app command error: %s", error)

            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)

    async def setup_hook(self) -> None:
        init_db(self.config)
        setup_commands(self, self.config)
        self._minute_task = start_minute_scheduler(self, self.config)
        self._hourly_task = start_hourly_scheduler(self, self.config)

    async def on_ready(self) -> None:
        guild_obj = discord.Object(id=self.config.guild_id)
        self.tree.clear_commands(guild=guild_obj)
        synced = await self.tree.sync(guild=guild_obj)
        _LOGGER.info("synced %d commands for guild %s", len(synced), self.config.guild_id)
        command_names = [command.name for command in self.tree.get_commands(guild=None)]
        _LOGGER.info("available global commands: %s", command_names)
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
