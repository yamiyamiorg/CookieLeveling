import inspect
import logging

import discord
from discord import app_commands

from cookieleveling.commands.handlers import (
    handle_optin,
    handle_optout,
    handle_level,
    handle_rankboard_set,
    handle_hostboard_set,
)
from cookieleveling.config.config import Config

_LOGGER = logging.getLogger(__name__)


def setup_commands(bot: discord.Client, config: Config) -> None:
    tree = bot.tree

    @tree.command(name="optout", description="Opt out of earning XP")
    async def optout(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, config, lambda: handle_optout(config, interaction.user.id)
        )

    @tree.command(name="optin", description="Opt in to earning XP")
    async def optin(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, config, lambda: handle_optin(config, interaction.user.id)
        )

    @tree.command(name="level", description="Show your level")
    async def level(interaction: discord.Interaction) -> None:
        if not _is_allowed_guild(interaction, config):
            await _reject_outside_guild(interaction)
            return
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

    @rankboard_group.command(name="set", description="Set rankboard messages")
    @app_commands.default_permissions(manage_guild=True)
    async def rankboard_set(interaction: discord.Interaction) -> None:
        if not _is_allowed_guild(interaction, config):
            await _reject_outside_guild(interaction)
            return
        if not _has_manage_guild(interaction):
            await _reject_missing_permission(interaction)
            return
        await _run_command(
            interaction, config, lambda: handle_rankboard_set(bot, config, interaction.channel)
        )

    tree.add_command(rankboard_group)

    hostboard_group = app_commands.Group(
        name="hostboard", description="Hostboard commands"
    )

    @hostboard_group.command(name="set", description="Set hostboard messages")
    @app_commands.default_permissions(manage_guild=True)
    async def hostboard_set(interaction: discord.Interaction) -> None:
        if not _is_allowed_guild(interaction, config):
            await _reject_outside_guild(interaction)
            return
        if not _has_manage_guild(interaction):
            await _reject_missing_permission(interaction)
            return
        await _run_command(
            interaction, config, lambda: handle_hostboard_set(bot, config, interaction.channel)
        )

    tree.add_command(hostboard_group)


async def _run_command(
    interaction: discord.Interaction, config: Config, handler
) -> None:
    if not _is_allowed_guild(interaction, config):
        await _reject_outside_guild(interaction)
        return
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


def _is_allowed_guild(interaction: discord.Interaction, config: Config) -> bool:
    return interaction.guild_id == config.guild_id


async def _reject_outside_guild(interaction: discord.Interaction) -> None:
    message = "このサーバーでは使用できません。"
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def _has_manage_guild(interaction: discord.Interaction) -> bool:
    member = interaction.user
    if isinstance(member, discord.Member):
        return member.guild_permissions.manage_guild
    return False


async def _reject_missing_permission(interaction: discord.Interaction) -> None:
    message = "権限が不足しています。"
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)
