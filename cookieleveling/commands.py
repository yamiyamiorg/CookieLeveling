import discord
from discord import app_commands

from .config import Config
from datetime import datetime, timezone

from .db import (
    fetch_guild_settings,
    fetch_rank_role_snapshot,
    fetch_user,
    get_connection,
    grant_xp,
    set_optout,
    set_voice_state,
    set_xp,
    upsert_guild_settings,
)
from .debug_mutations import ensure_debug_mutations
from .rankboard_publisher import send_rankboard_message, update_rankboard
from .ranker import compute_top10
from .role_sync import update_rank_roles
from .xp_engine import tick_minute
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

    @debug_group.command(name="grantxp", description="Grant XP to a user")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User", season="Season XP", lifetime="Lifetime XP")
    async def debug_grantxp(
        interaction: discord.Interaction, target: discord.User, season: int, lifetime: int
    ) -> None:
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        now = datetime.now(timezone.utc).isoformat()
        grant_xp(config.guild_id, target.id, season, lifetime, now)
        await interaction.response.send_message("XP granted.", ephemeral=True)

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
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        now = datetime.now(timezone.utc).isoformat()
        set_xp(config.guild_id, target.id, season, lifetime, rem_lifetime, now)
        await interaction.response.send_message("XP set.", ephemeral=True)

    @debug_group.command(name="setvc", description="Force VC state for a user")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="User", in_vc="True if in voice")
    async def debug_setvc(
        interaction: discord.Interaction, target: discord.User, in_vc: bool
    ) -> None:
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        joined_at = datetime.now(timezone.utc).isoformat() if in_vc else None
        set_voice_state(config.guild_id, target.id, in_vc, joined_at)
        await interaction.response.send_message("VC state updated.", ephemeral=True)

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

    @debug_group.command(name="roles", description="Show role sync status")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_roles(interaction: discord.Interaction) -> None:
        snapshot = fetch_rank_role_snapshot(config.guild_id)
        snapshot_json = snapshot["last_snapshot_json"] if snapshot else "[]"
        content = (
            f"ROLE_SEASON_1={config.role_season_1} "
            f"ROLE_SEASON_2={config.role_season_2} "
            f"ROLE_SEASON_3={config.role_season_3} "
            f"ROLE_SEASON_4={config.role_season_4} "
            f"ROLE_SEASON_5={config.role_season_5} "
            f"ROLE_SEASON_TOP10={config.role_season_top10} "
            f"snapshot={snapshot_json}"
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

    @tick_group.command(name="minute", description="Run minute XP tick once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_minute(interaction: discord.Interaction) -> None:
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        updated = tick_minute(config.guild_id)
        await interaction.response.send_message(
            f"Minute tick updated {updated} users.", ephemeral=True
        )

    @tick_group.command(name="roles", description="Run role sync once")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_tick_roles(interaction: discord.Interaction) -> None:
        if not ensure_debug_mutations(config):
            await interaction.response.send_message(
                "DEBUG_MUTATIONS=1 required.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        updated = await update_rank_roles(bot, config)
        message = "Roles updated." if updated else "Roles not configured."
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
