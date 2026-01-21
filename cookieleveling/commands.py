import discord
from discord import app_commands

from .config import Config
from .db import (
    fetch_guild_settings,
    fetch_user,
    get_connection,
    set_optout,
    upsert_guild_settings,
)
from .debug_mutations import ensure_debug_mutations
from .rankboard_publisher import send_rankboard_message, update_rankboard
from .ranker import compute_top10
from .voice_tracker import get_voice_debug_lines


def setup_commands(bot: discord.Client, config: Config) -> None:
    tree = bot.tree
    guild = discord.Object(id=config.guild_id)

    @tree.command(name="optout", description="Opt out of earning XP", guild=guild)
    async def optout(interaction: discord.Interaction) -> None:
        set_optout(config.guild_id, interaction.user.id, True)
        await interaction.response.send_message("Opted out.", ephemeral=True)

    @tree.command(name="optin", description="Opt in to earning XP", guild=guild)
    async def optin(interaction: discord.Interaction) -> None:
        set_optout(config.guild_id, interaction.user.id, False)
        await interaction.response.send_message("Opted in.", ephemeral=True)

    rankboard_group = app_commands.Group(name="rankboard", description="Rankboard commands")
    debug_group = app_commands.Group(name="debug", description="Debug commands")

    @debug_group.command(name="status", description="Show bot status")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_status(interaction: discord.Interaction) -> None:
        conn = get_connection()
        row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
        schema_version = row["schema_version"] if row else "unknown"
        await interaction.response.send_message(
            f"OK. schema_version={schema_version}", ephemeral=True
        )

    @debug_group.command(name="vc", description="Show voice state snapshot")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_vc(interaction: discord.Interaction) -> None:
        lines = get_voice_debug_lines(config.guild_id)
        content = "VC snapshot:\\n" + "\\n".join(lines)
        await interaction.response.send_message(content, ephemeral=True)

    @debug_group.command(name="user", description="Show user XP state")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User to inspect")
    async def debug_user(
        interaction: discord.Interaction, target: discord.User
    ) -> None:
        row = fetch_user(config.guild_id, target.id)
        if row is None:
            await interaction.response.send_message("User not found.", ephemeral=True)
            return
        content = (
            "user_id={user_id} season_xp={season_xp} lifetime_xp={lifetime_xp} "
            "rem_lifetime={rem_lifetime:.3f} optout={optout} is_in_vc={is_in_vc} "
            "joined_at={joined_at} last_earned_at={last_earned_at}"
        ).format(**row)
        await interaction.response.send_message(content, ephemeral=True)

    @debug_group.command(name="top10", description="Show top 10 ranking snapshot")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_top10(interaction: discord.Interaction) -> None:
        top10 = compute_top10(config.guild_id)
        if not top10:
            await interaction.response.send_message("No ranking data.", ephemeral=True)
            return
        lines = []
        for idx, entry in enumerate(top10, start=1):
            last_earned = (
                entry["last_earned_at"].isoformat()
                if entry["last_earned_at"]
                else "none"
            )
            lines.append(
                f"{idx}. user_id={entry['user_id']} season_xp={entry['season_xp']} "
                f"active_seconds={entry['active_seconds']} last_earned_at={last_earned}"
            )
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @debug_group.command(name="rankboard", description="Show rankboard settings")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_rankboard(interaction: discord.Interaction) -> None:
        settings = fetch_guild_settings(config.guild_id)
        if settings is None:
            await interaction.response.send_message("Rankboard not configured.", ephemeral=True)
            return
        content = (
            f"channel_id={settings['rankboard_channel_id']} "
            f"message_id={settings['rankboard_message_id']} "
            f"updated_at={settings['updated_at']}"
        )
        await interaction.response.send_message(content, ephemeral=True)

    tick_group = app_commands.Group(name="tick", description="Run debug ticks")

    @tick_group.command(name="rankboard", description="Run rankboard update once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_rankboard(interaction: discord.Interaction) -> None:
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        updated = await update_rankboard(bot, config)
        message = "Rankboard updated." if updated else "Rankboard not configured."
        await interaction.followup.send(message, ephemeral=True)

    debug_group.add_command(tick_group)

    @rankboard_group.command(name="set", description="Set rankboard message")
    @app_commands.checks.has_permissions(administrator=True)
    async def rankboard_set(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        channel = interaction.channel
        if isinstance(channel, discord.Thread):
            channel = channel.parent

        if not isinstance(channel, discord.TextChannel):
            await interaction.followup.send("Channel not supported.", ephemeral=True)
            return

        settings = fetch_guild_settings(config.guild_id)
        if settings and settings["rankboard_message_id"]:
            await _mark_rankboard_moved(
                bot,
                settings["rankboard_channel_id"],
                settings["rankboard_message_id"],
                channel.id,
            )

        message = await send_rankboard_message(bot, config, channel)
        upsert_guild_settings(config.guild_id, channel.id, message.id)
        await interaction.followup.send(
            f"Rankboard set in <#{channel.id}>.", ephemeral=True
        )

    tree.add_command(debug_group, guild=guild)
    tree.add_command(rankboard_group, guild=guild)


async def _mark_rankboard_moved(
    bot: discord.Client, channel_id: int, message_id: int, new_channel_id: int
) -> None:
    channel = bot.get_channel(channel_id)
    if channel is None or not isinstance(channel, discord.TextChannel):
        return
    try:
        message = await channel.fetch_message(message_id)
    except discord.NotFound:
        return
    await message.edit(
        content=f"Moved to <#{new_channel_id}>.",
        embeds=[],
        attachments=[],
    )
