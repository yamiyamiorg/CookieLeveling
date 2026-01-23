import inspect
import logging

import discord
from discord import app_commands

from .command_handlers import (
    handle_debug_grantxp,
    handle_debug_rankboard,
    handle_debug_roles,
    handle_debug_setvc,
    handle_debug_setxp,
    handle_debug_status,
    handle_debug_top10,
    handle_debug_user,
    handle_debug_vc,
    handle_optin,
    handle_optout,
    handle_level,
    handle_rankboard_set,
    handle_tick_minute,
    handle_tick_rankboard,
    handle_tick_roles,
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

    rankboard_group = app_commands.Group(name="rankboard", description="Rankboard commands")
    debug_group = app_commands.Group(name="debug", description="Debug commands")

    @debug_group.command(name="status", description="Show bot status")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_status(interaction: discord.Interaction) -> None:
        await _run_command(interaction, handle_debug_status)

    @debug_group.command(name="vc", description="Show voice state snapshot")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_vc(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_debug_vc(config))

    @debug_group.command(name="user", description="Show user XP state")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User to inspect")
    async def debug_user(
        interaction: discord.Interaction, target: discord.User
    ) -> None:
        await _run_command(
            interaction, lambda: handle_debug_user(config, target.id)
        )

    @debug_group.command(name="grantxp", description="Grant XP to a user")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User", season="Season XP", lifetime="Lifetime XP")
    async def debug_grantxp(
        interaction: discord.Interaction, target: discord.User, season: int, lifetime: int
    ) -> None:
        await _run_command(
            interaction,
            lambda: handle_debug_grantxp(config, target.id, season, lifetime),
        )

    @debug_group.command(name="setxp", description="Set XP for a user")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        target="User",
        season="Season XP",
        lifetime="Lifetime XP",
        rem_lifetime="Remainder for lifetime XP",
    )
    async def debug_setxp(
        interaction: discord.Interaction,
        target: discord.User,
        season: int,
        lifetime: int,
        rem_lifetime: float | None = None,
    ) -> None:
        await _run_command(
            interaction,
            lambda: handle_debug_setxp(
                config, target.id, season, lifetime, rem_lifetime
            ),
        )

    @debug_group.command(name="setvc", description="Force VC state for a user")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User", in_vc="True if in voice")
    async def debug_setvc(
        interaction: discord.Interaction, target: discord.User, in_vc: bool
    ) -> None:
        await _run_command(
            interaction, lambda: handle_debug_setvc(config, target.id, in_vc)
        )

    @debug_group.command(name="top10", description="Show top 10 ranking snapshot")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_top10(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_debug_top10(config))

    @debug_group.command(name="rankboard", description="Show rankboard settings")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_rankboard(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_debug_rankboard(config))

    @debug_group.command(name="roles", description="Show role sync status")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_roles(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_debug_roles(config))

    tick_group = app_commands.Group(name="tick", description="Run debug ticks")

    @tick_group.command(name="rankboard", description="Run rankboard update once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_rankboard(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, lambda: handle_tick_rankboard(bot, config)
        )

    @tick_group.command(name="minute", description="Run minute XP tick once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_minute(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_tick_minute(config))

    @tick_group.command(name="roles", description="Run role sync once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_roles(interaction: discord.Interaction) -> None:
        await _run_command(interaction, lambda: handle_tick_roles(bot, config))

    debug_group.add_command(tick_group)

    @rankboard_group.command(name="set", description="Set rankboard message")
    @app_commands.checks.has_permissions(administrator=True)
    async def rankboard_set(interaction: discord.Interaction) -> None:
        await _run_command(
            interaction, lambda: handle_rankboard_set(bot, config, interaction.channel)
        )

    tree.add_command(debug_group)
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
