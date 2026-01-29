import inspect
import logging

import discord
from discord import app_commands

from .command_handlers import (
    handle_optin,
    handle_optout,
    handle_level,
    handle_rankboard_set,
)
from .config import Config

_LOGGER = logging.getLogger(__name__)


def setup_commands(bot: discord.Client, config: Config) -> None:
    tree = bot.tree

    @tree.command(name="optout", description="Opt out of earning XP")
    async def optout(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, lambda: handle_optout(config, interaction.user.id)
        )

    @tree.command(name="optin", description="Opt in to earning XP")
    async def optin(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, lambda: handle_optin(config, interaction.user.id)
        )

    @tree.command(name="level", description="Show your level")
    async def level(interaction: discord.Interaction) -> None:
        await _defer_ephemeral(interaction)
        try:
            rendered, error = await handle_level(config, interaction.user)
            if error:
                await _send_ephemeral(interaction, error)
                return
            if rendered is None:
                await _send_ephemeral(interaction, "画像生成に失敗しました。")
                return
            await interaction.followup.send(
                content="",
                embeds=[],
                file=rendered,
                ephemeral=True,
            )
        except Exception:
            _LOGGER.exception("level command failed")
            await _send_ephemeral(interaction, "エラーが発生しました。")

    rankboard_group = app_commands.Group(
        name="rankboard", description="Rankboard commands"
    )

    @rankboard_group.command(name="set", description="Set rankboard message")
    @app_commands.checks.has_permissions(administrator=True)
    async def rankboard_set(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, lambda: handle_rankboard_set(bot, config, interaction.channel)
        )

    tree.add_command(rankboard_group)


async def _run_command(
    interaction: discord.Interaction, handler
) -> None:
    await _defer_ephemeral(interaction)
    try:
        result = handler()
        if inspect.isawaitable(result):
            result = await result
        await _send_ephemeral(interaction, result)
    except Exception:
        _LOGGER.exception("command failed")
        await _send_ephemeral(interaction, "エラーが発生しました。")


async def _defer_ephemeral(interaction: discord.Interaction) -> None:
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True)


async def _send_ephemeral(
    interaction: discord.Interaction, message: str
) -> None:
    await interaction.followup.send(message, ephemeral=True)
